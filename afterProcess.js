// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import fs from "fs/promises";
import puppeteer from "puppeteer";
import { mirrorMessageToSupabase } from "./supabaseSync.js";

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

/**
 * MAIN ENTRY
 * index.js の saveMessageDoc() 直後に呼ばれる
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  const tTotal = Date.now();
  console.log("[after] start", { messageId });

  try {
    const msgRef = firestore.collection("messages").doc(messageId);

    const tGetMsg = Date.now();
    const msgSnap = await msgRef.get();
    console.log(
      "[after] firestore.get message",
      { messageId },
      Date.now() - tGetMsg,
      "ms"
    );

    if (!msgSnap.exists) {
      console.log("[after] message not found, skip", { messageId });
      return;
    }

    const data = msgSnap.data();
    const attachments = data.attachments || [];
    const isFax = data.messageType === "fax";

    // === 0) OCR（テキストだけ取得）===
    const tOcr = Date.now();
    const { fullOcrText } = await runOcr(attachments);
    console.log(
      "[after] runOcr",
      { messageId, attachmentsCount: attachments.length },
      Date.now() - tOcr,
      "ms"
    );

    // === 本文候補プール（顧客特定などに使用） ===
    const bodyPool = [
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（毎回新規発番）===
    const tMgmt = Date.now();
    const managementNo = await ensureManagementNo7(firestore);
    console.log(
      "[after] ensureManagementNo7",
      { messageId, managementNo },
      Date.now() - tMgmt,
      "ms"
    );

    // === 2) 顧客特定 ===
    const tDetect = Date.now();
    const head100 = String(fullOcrText || bodyPool).slice(0, 100);
    let customer =
      (head100 && (await detectCustomer(firestore, head100))) ||
      (await detectCustomer(firestore, bodyPool));
    console.log(
      "[after] detectCustomer",
      {
        messageId,
        found: !!customer,
        customerId: customer?.id || null,
        customerName: customer?.name || null,
      },
      Date.now() - tDetect,
      "ms"
    );

    // === 2.5) メインPDF ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null; // サムネは使わないがフィールドは残す

    const tMainPdf = Date.now();
    if (bucket) {
      if (isFax) {
        // 📠 FAX → 添付PDFをそのままメインPDFとして扱う
        const firstAttachment = (attachments || []).find(
          (p) => typeof p === "string"
        );
        if (firstAttachment) {
          mainPdfPath = firstAttachment;
        }
        console.log(
          "[after] mainPdf (fax)",
          { messageId, mainPdfPath },
          Date.now() - tMainPdf,
          "ms"
        );
      } else {
        // ✉ メール → HTML → PDF（サムネ無し）
        const htmlSource =
          data.textHtml ||
          (data.textPlain ? `<pre>${String(data.textPlain)}</pre>` : null);

        if (htmlSource) {
          try {
            const tRender = Date.now();
            mainPdfPath = await renderMailHtmlToPdf({
              bucket,
              messageId,
              html: htmlSource,
            });
            console.log(
              "[after] renderMailHtmlToPdf",
              { messageId, mainPdfPath },
              Date.now() - tRender,
              "ms"
            );
          } catch (e) {
            console.error("renderMailHtmlToPdf failed:", e);
          }
        } else {
          console.log(
            "[after] renderMailHtmlToPdf skipped (no htmlSource)",
            { messageId }
          );
        }
      }
    } else {
      console.log("[after] mainPdf skipped (no bucket)", { messageId });
    }

    // === 3) Firestore 更新 ===
    const tSet = Date.now();
    await msgRef.set(
      {
        managementNo,
        parentManagementNo: managementNo,
        customerId: customer?.id || null,
        customerName: customer?.name || null,
        ocr: {
          fullText: fullOcrText || "",
          at: Date.now(),
        },
        mainPdfPath: mainPdfPath || null,
        mainPdfThumbnailPath: mainPdfThumbnailPath || null,
        processedAt: Date.now(),
      },
      { merge: true }
    );
    console.log(
      "[after] firestore.set (processed fields)",
      { messageId },
      Date.now() - tSet,
      "ms"
    );

    // === 4) Supabase ミラー ===
    const tSupabase = Date.now();
    mirrorMessageToSupabase({
      messageId,
      data: {
        ...data,
        ocr: { fullText: fullOcrText },
        mainPdfPath,
        mainPdfThumbnailPath,
      },
      managementNo,
      customer,
    });
    console.log(
      "[after] mirrorMessageToSupabase called",
      { messageId, managementNo },
      Date.now() - tSupabase,
      "ms"
    );

    console.log(
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} total=${Date.now() - tTotal}ms`
    );
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/* ================= 管理番号 ================= */

export async function ensureManagementNo7(firestore) {
  const FIXED_DIGITS = 7;
  const seqRef = firestore
    .collection("system_configs")
    .doc("sequence_managementNo_global");

  const tTx = Date.now();
  const result = await firestore.runTransaction(async (tx) => {
    const snap = await tx.get(seqRef);
    const current = snap.exists && snap.data()?.v ? Number(snap.data().v) : 0;

    let next = current + 1;
    const max = Math.pow(16, FIXED_DIGITS);
    if (next >= max) next = 1;

    tx.set(
      seqRef,
      { v: next, digits: FIXED_DIGITS, updatedAt: Date.now() },
      { merge: true }
    );

    return next.toString(16).toUpperCase().padStart(FIXED_DIGITS, "0");
  });
  console.log(
    "[after] ensureManagementNo7 transaction total",
    Date.now() - tTx,
    "ms"
  );
  return result;
}

/* ================= OCR ================= */

async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "" };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random()
    .toString(36)
    .slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const tRequest = Date.now();
  const [op] = await client.asyncBatchAnnotateFiles({
    requests: [
      {
        inputConfig: { gcsSource: { uri: gcsUri }, mimeType },
        features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
        outputConfig: { gcsDestination: { uri: outUri }, batchSize: 1 },
        pages: [1],
      },
    ],
  });
  console.log(
    "[ocr] asyncBatchAnnotateFiles request",
    { gcsUri, mimeType },
    Date.now() - tRequest,
    "ms"
  );

  const tOp = Date.now();
  await op.promise();
  console.log(
    "[ocr] op.promise",
    { gcsUri },
    Date.now() - tOp,
    "ms"
  );

  const tListFiles = Date.now();
  const [files] = await storage
    .bucket(info.bucket)
    .getFiles({ prefix: tmpPrefix });
  console.log(
    "[ocr] getFiles",
    { gcsUri, tmpPrefix, filesCount: files.length },
    Date.now() - tListFiles,
    "ms"
  );
  if (!files.length) return { text: "" };

  const tDownload = Date.now();
  const [buf] = await files[0].download();
  console.log(
    "[ocr] download result file",
    { gcsUri },
    Date.now() - tDownload,
    "ms"
  );

  const tCleanup = Date.now();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));
  console.log(
    "[ocr] cleanup temp files",
    { gcsUri },
    Date.now() - tCleanup,
    "ms"
  );

  const tParse = Date.now();
  const json = JSON.parse(buf.toString());
  const resp = json.responses?.[0];
  const text = resp?.fullTextAnnotation?.text || "";
  console.log(
    "[ocr] parse json & extract text",
    { gcsUri, hasText: !!text },
    Date.now() - tParse,
    "ms"
  );

  return { text };
}

async function ocrImageText(gcsUri) {
  const tOcr = Date.now();
  const [res] = await client.documentTextDetection(gcsUri);
  const text =
    res.fullTextAnnotation?.text ||
    res.textAnnotations?.[0]?.description ||
    "";
  console.log(
    "[ocr] documentTextDetection",
    { gcsUri, hasText: !!text },
    Date.now() - tOcr,
    "ms"
  );
  return { text };
}

async function runOcr(attachments) {
  const tTotal = Date.now();
  let fullOcrText = "";

  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

    const tOne = Date.now();
    try {
      let r;
      if (lower.endsWith(".pdf")) {
        r = await ocrFirstPageFromFile(uri, "application/pdf");
      } else if (lower.endsWith(".tif") || lower.endsWith(".tiff")) {
        r = await ocrFirstPageFromFile(uri, "image/tiff");
      } else {
        r = await ocrImageText(uri);
      }

      if (r?.text) {
        fullOcrText += (fullOcrText ? "\n" : "") + r.text;
      }
      console.log(
        "[ocr] one attachment done",
        { uri, hasText: !!r?.text },
        Date.now() - tOne,
        "ms"
      );
    } catch (e) {
      console.warn("OCR failed:", uri, e);
    }
  }

  console.log(
    "[ocr] runOcr total",
    { attachmentsCount: (attachments || []).length },
    Date.now() - tTotal,
    "ms"
  );

  return {
    fullOcrText,
  };
}

/* ================= 顧客特定 ================= */

async function detectCustomer(firestore, sourceText) {
  const tTotal = Date.now();
  try {
    const tGet = Date.now();
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    console.log(
      "[customer] firestore.get system_customers_master",
      Date.now() - tGet,
      "ms"
    );
    if (!snap.exists) return null;

    const arr = snap.data()?.customers || [];
    const s = (sourceText || "").replace(/\s+/g, "").toLowerCase();

    const tScan = Date.now();
    for (const c of arr) {
      const aliases = (c.aliases || []).map((a) => String(a).toLowerCase());
      if (
        aliases.some((a) =>
          s.includes(a.replace(/\s+/g, ""))
        )
      ) {
        console.log(
          "[customer] matched",
          { customerId: c.id, customerName: c.name },
          "scan in",
          Date.now() - tScan,
          "ms"
        );
        console.log(
          "[customer] detectCustomer total",
          Date.now() - tTotal,
          "ms"
        );
        return { id: c.id, name: c.name };
      }
    }
    console.log(
      "[customer] no match",
      { customersCount: arr.length },
      "scan in",
      Date.now() - tScan,
      "ms"
    );
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  console.log(
    "[customer] detectCustomer total (null)",
    Date.now() - tTotal,
    "ms"
  );
  return null;
}

/* ================= HTML → PDF（メール用） ================= */

async function renderMailHtmlToPdf({ bucket, messageId, html }) {
  const tTotal = Date.now();
  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;

  const tLaunch = Date.now();
  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  console.log(
    "[pdf] puppeteer.launch",
    { messageId },
    Date.now() - tLaunch,
    "ms"
  );

  try {
    const tPage = Date.now();
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });
    console.log(
      "[pdf] page.setContent + networkidle0",
      { messageId },
      Date.now() - tPage,
      "ms"
    );

    const tPdf = Date.now();
    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
    });
    console.log(
      "[pdf] page.pdf (local file)",
      { messageId, pdfTmp },
      Date.now() - tPdf,
      "ms"
    );
  } finally {
    const tClose = Date.now();
    await browser.close().catch(() => {});
    console.log(
      "[pdf] browser.close",
      { messageId },
      Date.now() - tClose,
      "ms"
    );
  }

  const tRead = Date.now();
  const buf = await fs.readFile(pdfTmp);
  console.log(
    "[pdf] fs.readFile",
    { messageId, pdfTmp },
    Date.now() - tRead,
    "ms"
  );

  const objectPath = `mail_rendered/${safeId}.pdf`;

  const tUpload = Date.now();
  await bucket.file(objectPath).save(buf, {
    resumable: false,
    contentType: "application/pdf",
  });
  console.log(
    "[pdf] GCS save",
    { messageId, objectPath },
    Date.now() - tUpload,
    "ms"
  );

  console.log(
    "[pdf] renderMailHtmlToPdf total",
    { messageId, objectPath },
    Date.now() - tTotal,
    "ms"
  );

  return `gs://${bucket.name}/${objectPath}`;
}

/* ================= Helper ================= */

function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

function sanitizeId(id) {
  return String(id || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 100);
}
