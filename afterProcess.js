// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import fs from "fs/promises";
import puppeteer from "puppeteer";
import { mirrorMessageToSupabase } from "./supabaseSync.js";

import { Firestore } from "@google-cloud/firestore"; // ★ 追加

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

/**
 * MAIN ENTRY
 * index.js の saveMessageDoc() 直後に呼ばれる
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];
    const isFax = data.messageType === "fax";

     // === 0) OCR（テキストだけ取得）===
    let fullOcrText = "";
    if (isFax) {
      const r = await runOcr(attachments);
      fullOcrText = r.fullOcrText || "";
    }



    // === 本文候補プール（顧客特定などに使用） ===
    const bodyPool = [
      data.subject || "",     // ★ これ追加
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");


    // === 1) 管理番号（毎回新規発番）===
    const managementNo = await ensureManagementNo7(firestore);

    // === 2) 顧客特定 ===
    const head100 = String(fullOcrText || bodyPool).slice(0, 100);
    let customer =
      (head100 && (await detectCustomer(firestore, head100))) ||
      (await detectCustomer(firestore, bodyPool));

    // === 2.5) メインPDF ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null; // サムネは使わないがフィールドは残す

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
          (data.textPlain
            ? `<pre>${String(data.textPlain)}</pre>`
            : null);

        if (htmlSource) {
          try {
            mainPdfPath = await renderMailHtmlToPdf({
              bucket,
              messageId,
              html: htmlSource,
            });
          } catch (e) {
            console.error("renderMailHtmlToPdf failed:", e);
          }
        }
      }
    }

    // === 3) Firestore 更新 ===
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

    // === 4) Supabase ミラー ===
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
      `✅ afterProcess done id=${messageId} mgmt=${managementNo}`
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

async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "" };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random()
    .toString(36)
    .slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

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

  await op.promise();

  const [files] = await storage
    .bucket(info.bucket)
    .getFiles({ prefix: tmpPrefix });
  if (!files.length) return { text: "" };

  const [buf] = await files[0].download();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const resp = json.responses?.[0];
  const text = resp?.fullTextAnnotation?.text || "";

  return { text };
}

async function ocrImageText(gcsUri) {
  const [res] = await client.documentTextDetection(gcsUri);
  const text =
    res.fullTextAnnotation?.text ||
    res.textAnnotations?.[0]?.description ||
    "";
  return { text };
}

async function runOcr(attachments) {
  let fullOcrText = "";

  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

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
    } catch (e) {
      console.warn("OCR failed:", uri, e);
    }
  }

  return {
    fullOcrText,
  };
}

/* ================= 顧客特定 ================= */

async function detectCustomer(_firestore, sourceText) {
  try {
    // どんなテキストで検索しているか、先頭だけログに出す
    console.log("detectCustomer sourceText head:", String(sourceText).slice(0, 200));

    const snap = await customerDb
      .collection("jsons")
      .doc("Client Search")
      .get();

    console.log("Client Search exists?", snap.exists);

    if (!snap.exists) return null;

    const raw = snap.data();
    console.log("Client Search root keys:", Object.keys(raw || {}));

    // ① ルート直下に tables があるパターン
    // ② 何かの下にネストされているパターン（例: { data: { tables: [...] } }）
    let data = raw;
    if (!Array.isArray(data.tables)) {
      // よくあるラップフィールド名を順に試す（必要なら増やせる）
      if (raw.data && Array.isArray(raw.data.tables)) {
        data = raw.data;
      } else if (raw.sheet && Array.isArray(raw.sheet.tables)) {
        data = raw.sheet;
      }
    }

    if (!Array.isArray(data.tables) || !data.tables[0]?.matrix) {
      console.log("NO tables[0].matrix found in Client Search");
      return null;
    }

    const matrix = data.tables[0].matrix;
    if (!matrix || matrix.length < 2) {
      console.log("matrix empty or no data rows");
      return null;
    }

    const header = matrix[0];
    console.log("Client Search header row:", header);

    const idx = (colName) => header.indexOf(colName);

    const colId          = idx("id");
    const colName        = idx("name");
    const colMailAliases = idx("mail_aliases");
    const colFaxAliases  = idx("fax_aliases");
    const colNameAliases = idx("name_aliases");

    console.log("col indexes:", {
      colId,
      colName,
      colMailAliases,
      colFaxAliases,
      colNameAliases,
    });

    if (colId === -1 || colName === -1) {
      console.log("id or name column not found");
      return null;
    }

    const normalize = (str) =>
      String(str || "").toLowerCase().replace(/\s+/g, "");

    const normalizeDigits = (str) =>
      String(str || "").replace(/[^\d]/g, "");

    const textNorm   = normalize(sourceText);
    const textDigits = normalizeDigits(sourceText);

    const split = (v) =>
      String(v || "")
        .split(/[,\s、;／]+/)
        .map((x) => x.trim())
        .filter(Boolean);

    const rows = matrix.slice(1).map((row) => {
      const id   = row[colId];
      const name = row[colName];

      const mailAliases = colMailAliases !== -1 ? split(row[colMailAliases]) : [];
      const faxAliases  = colFaxAliases  !== -1 ? split(row[colFaxAliases])  : [];
      const nameAliases = colNameAliases !== -1 ? split(row[colNameAliases]) : [];

      return { id, name, mailAliases, faxAliases, nameAliases };
    });

    // ① mail エイリアスに含むか（最優先）
    for (const r of rows) {
      for (const a of r.mailAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) {
          console.log("match by MAIL alias:", { id: r.id, name: r.name, alias: a });
          return { id: r.id, name: r.name };
        }
      }
    }

    // ② fax 番号エイリアス（数字だけでマッチ）
    for (const r of rows) {
      for (const a of r.faxAliases) {
        const aDigits = normalizeDigits(a);
        if (aDigits && textDigits.includes(aDigits)) {
          console.log("match by FAX alias:", { id: r.id, name: r.name, alias: a, aDigits, textDigits });
          return { id: r.id, name: r.name };
        }
      }
    }

    // ③ 名前エイリアス（name_aliases のみ）
    for (const r of rows) {
      for (const a of r.nameAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) {
          console.log("match by NAME alias:", { id: r.id, name: r.name, alias: a });
          return { id: r.id, name: r.name };
        }
      }
    }

    console.log("no customer matched");

  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}


/* ================= HTML → PDF（メール用） ================= */

async function renderMailHtmlToPdf({ bucket, messageId, html }) {
  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });

    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
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

