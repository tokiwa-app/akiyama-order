// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import fs from "fs/promises";
import puppeteer from "puppeteer";
import { mirrorMessageToSupabase } from "./supabaseSync.js";
import { Firestore } from "@google-cloud/firestore";

// akiyama-system データベース（取引先マスタ用）
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;
const customerDb = new Firestore(
  FIREBASE_PROJECT_ID
    ? { projectId: FIREBASE_PROJECT_ID, databaseId: "akiyama-system" }
    : { databaseId: "akiyama-system" }
);

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

function msSince(t0) {
  return Date.now() - t0;
}
function log(reqId, msg, extra = {}) {
  console.log(`[${reqId || "no-req"}] ${msg}`, extra);
}

// タイムアウト付与（「止まる」を「どこで止まったか分かる」にする）
function withTimeout(promise, ms, label, reqId) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => {
        reject(new Error(`TIMEOUT ${label} after ${ms}ms`));
      }, ms)
    ),
  ]).catch((e) => {
    log(reqId, `❌ ${label} timed out/error`, { message: e?.message });
    throw e;
  });
}

/**
 * MAIN ENTRY
 * index.js の saveMessageDoc() 直後に呼ばれる
 */
export async function runAfterProcess({ messageId, firestore, bucket, reqId }) {
  const T = Date.now();
  log(reqId, "afterProcess START", { messageId });

  try {
    const t0 = Date.now();
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    log(reqId, "Firestore get message", { ms: msSince(t0), messageId });

    if (!msgSnap.exists) {
      log(reqId, "message doc not found; return", { messageId });
      return;
    }

    const data = msgSnap.data();
    const attachments = data.attachments || [];
    const isFax = data.messageType === "fax";

    log(reqId, "messageType", { isFax, messageType: data.messageType, attachmentsCount: attachments.length });

    // === 0) OCR（テキストだけ取得）===
    let fullOcrText = "";
    if (isFax) {
      const t1 = Date.now();
      const r = await runOcr(attachments, reqId);
      fullOcrText = r.fullOcrText || "";
      log(reqId, "runOcr TOTAL", { ms: msSince(t1), textLen: fullOcrText.length });
    }

    // === 本文候補プール（顧客特定などに使用） ===
    const bodyPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（毎回新規発番）===
    const t2 = Date.now();
    const managementNo = await ensureManagementNo7(firestore);
    log(reqId, "ensureManagementNo7", { ms: msSince(t2), managementNo });

    // === 2) 顧客特定 ===
    const t3 = Date.now();
    const head100 = String(fullOcrText || bodyPool).slice(0, 100);
    let customer =
      (head100 && (await detectCustomer(firestore, head100, reqId))) ||
      (await detectCustomer(firestore, bodyPool, reqId));
    log(reqId, "detectCustomer TOTAL", { ms: msSince(t3), customer });

    // === 2.5) メインPDF ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null;

    if (bucket) {
      if (isFax) {
        // 📠 FAX → 添付PDFをそのままメインPDFとして扱う
        const firstAttachment = (attachments || []).find(
          (p) => typeof p === "string"
        );
        if (firstAttachment) {
          mainPdfPath = firstAttachment;
        }
      } else {
        // ✉ メール → HTML → PDF（サムネ無し）
        const htmlSource =
          data.textHtml ||
          (data.textPlain ? `<pre>${String(data.textPlain)}</pre>` : null);

        if (htmlSource) {
          try {
            const tPDF = Date.now();
            mainPdfPath = await renderMailHtmlToPdf({
              bucket,
              messageId,
              html: htmlSource,
              reqId,
            });
            log(reqId, "renderMailHtmlToPdf", { ms: msSince(tPDF), mainPdfPath });
          } catch (e) {
            console.error("renderMailHtmlToPdf failed:", e);
          }
        }
      }
    }

    // === 3) Firestore 更新 ===
    const t4 = Date.now();
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
        processingAt: null,
      },
      { merge: true }
    );
    log(reqId, "Firestore set processed fields", { ms: msSince(t4) });

    // === 4) Supabase ミラー ===
    const t5 = Date.now();
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
    log(reqId, "mirrorMessageToSupabase queued", { ms: msSince(t5) });

    log(reqId, "afterProcess END", { ms: msSince(T), messageId, managementNo });
  } catch (e) {
    console.error("afterProcess error:", e);
    log(reqId, "afterProcess ERROR END", { ms: msSince(T), messageId, err: e?.message });
  }
}

/* ================= 管理番号 ================= */

export async function ensureManagementNo7(firestore) {
  const FIXED_DIGITS = 7;
  const seqRef = firestore
    .collection("system_configs")
    .doc("sequence_managementNo_global");

  return await firestore.runTransaction(async (tx) => {
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
}

/* ================= OCR ================= */

async function ocrFirstPageFromFile(gcsUri, mimeType, reqId) {
  const t0 = Date.now();
  log(reqId, "OCR: ocrFirstPageFromFile START", { gcsUri, mimeType });

  const info = parseGsUri(gcsUri);
  if (!info) return { text: "" };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random().toString(36).slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const t1 = Date.now();
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
  log(reqId, "OCR: Vision op created", { ms: msSince(t1) });

  // ★ここが最有力の詰まりポイント：明示タイムアウトで切り分け
  const t2 = Date.now();
  await withTimeout(op.promise(), 150000, "OCR: op.promise (Vision batch)", reqId); // 例: 150秒
  log(reqId, "OCR: op.promise DONE", { ms: msSince(t2) });

  const t3 = Date.now();
  const [files] = await storage.bucket(info.bucket).getFiles({ prefix: tmpPrefix });
  log(reqId, "OCR: GCS list DONE", { ms: msSince(t3), count: files.length });

  if (!files.length) {
    log(reqId, "OCR: no output files", { tmpPrefix });
    return { text: "" };
  }

  const t4 = Date.now();
  const [buf] = await files[0].download();
  log(reqId, "OCR: GCS download DONE", { ms: msSince(t4), bytes: buf.length });

  const t5 = Date.now();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));
  log(reqId, "OCR: GCS cleanup DONE", { ms: msSince(t5) });

  const json = JSON.parse(buf.toString());
  const resp = json.responses?.[0];
  const text = resp?.fullTextAnnotation?.text || "";

  log(reqId, "OCR: ocrFirstPageFromFile END", { ms: msSince(t0), textLen: text.length });
  return { text };
}

async function ocrImageText(gcsUri, reqId) {
  const t0 = Date.now();
  log(reqId, "OCR: ocrImageText START", { gcsUri });

  // ★ここもタイムアウト付ける（画像でも止まる場合があるため）
  const [res] = await withTimeout(
    client.documentTextDetection(gcsUri),
    60000,
    "OCR: documentTextDetection(image)",
    reqId
  );

  const text =
    res.fullTextAnnotation?.text ||
    res.textAnnotations?.[0]?.description ||
    "";

  log(reqId, "OCR: ocrImageText END", { ms: msSince(t0), textLen: text.length });
  return { text };
}

async function runOcr(attachments, reqId) {
  let fullOcrText = "";

  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

    const t = Date.now();
    log(reqId, "OCR: attachment START", { uri });

    try {
      let r;
      if (lower.endsWith(".pdf")) {
        r = await ocrFirstPageFromFile(uri, "application/pdf", reqId);
      } else if (lower.endsWith(".tif") || lower.endsWith(".tiff")) {
        r = await ocrFirstPageFromFile(uri, "image/tiff", reqId);
      } else {
        r = await ocrImageText(uri, reqId);
      }

      log(reqId, "OCR: attachment END", {
        uri,
        ms: msSince(t),
        textLen: (r?.text || "").length,
      });

      if (r?.text) {
        fullOcrText += (fullOcrText ? "\n" : "") + r.text;
      }
    } catch (e) {
      log(reqId, "OCR: attachment ERROR", { uri, ms: msSince(t), err: e?.message });
    }
  }

  return { fullOcrText };
}

/* ================= 顧客特定 ================= */

async function detectCustomer(_firestore, sourceText, reqId) {
  try {
    const t0 = Date.now();
    const snap = await customerDb.collection("jsons").doc("Client Search").get();
    log(reqId, "detectCustomer: fetched Client Search", { ms: msSince(t0), exists: snap.exists });

    if (!snap.exists) return null;

    const doc = snap.data();
    let sheet = doc.main;
    if (typeof sheet === "string") {
      try {
        sheet = JSON.parse(sheet);
      } catch (e) {
        log(reqId, "detectCustomer: JSON.parse failed", { err: e?.message });
        return null;
      }
    }

    const tables = sheet?.tables;
    const matrix = tables?.[0]?.matrix;
    if (!Array.isArray(matrix) || matrix.length < 2) return null;

    const header = matrix[0];
    const idx = (colName) => header.indexOf(colName);

    const colId = idx("id");
    const colName = idx("name");
    const colMailAliases = idx("mail_aliases");
    const colFaxAliases = idx("fax_aliases");
    const colNameAliases = idx("name_aliases");

    if (colId === -1 || colName === -1) return null;

    const normalize = (str) =>
      String(str || "").toLowerCase().replace(/\s+/g, "");
    const normalizeDigits = (str) =>
      String(str || "").replace(/[^\d]/g, "");

    const textNorm = normalize(sourceText);
    const textDigits = normalizeDigits(sourceText);

    const split = (v) =>
      String(v || "")
        .split(/[,\s、;／]+/)
        .map((x) => x.trim())
        .filter(Boolean);

    const rows = matrix.slice(1).map((row) => {
      const id = row[colId];
      const name = row[colName];
      const mailAliases = colMailAliases !== -1 ? split(row[colMailAliases]) : [];
      const faxAliases = colFaxAliases !== -1 ? split(row[colFaxAliases]) : [];
      const nameAliases = colNameAliases !== -1 ? split(row[colNameAliases]) : [];
      return { id, name, mailAliases, faxAliases, nameAliases };
    });

    for (const r of rows) {
      for (const a of r.mailAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) return { id: r.id, name: r.name };
      }
    }

    for (const r of rows) {
      for (const a of r.faxAliases) {
        const aDigits = normalizeDigits(a);
        if (aDigits && textDigits.includes(aDigits)) return { id: r.id, name: r.name };
      }
    }

    for (const r of rows) {
      for (const a of r.nameAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) return { id: r.id, name: r.name };
      }
    }
  } catch (e) {
    log(reqId, "detectCustomer error", { err: e?.message });
  }
  return null;
}

/* ================= HTML → PDF（メール用） ================= */

async function renderMailHtmlToPdf({ bucket, messageId, html, reqId }) {
  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();

    // もし mail も来る可能性があるなら、networkidle0 は避けるのが安全
    await page.setContent(html, { waitUntil: "domcontentloaded", timeout: 15000 });

    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
      timeout: 30000,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  const objectPath = `mail_rendered/${safeId}.pdf`;
  const buf = await fs.readFile(pdfTmp);

  await bucket.file(objectPath).save(buf, {
    resumable: false,
    contentType: "application/pdf",
  });

  log(reqId, "renderMailHtmlToPdf saved", { objectPath });
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
