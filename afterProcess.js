// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import puppeteer from "puppeteer";
import fs from "fs/promises";
import { mirrorMessageToSupabase } from "./supabaseSync.js";

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

    // === 0) OCR ===
    const { fullOcrText, firstPageRotation } = await runOcr(attachments);

    // === 本文候補プール ===
    const bodyPool = [
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（AKSNO → 重複チェック → generate）===
    const aksCandidate = extractAksNo7(bodyPool);
    let managementNo = null;

    if (aksCandidate) {
      const existingSnap = await firestore
        .collection("messages")
        .where("managementNo", "==", aksCandidate)
        .limit(1)
        .get();
      if (!existingSnap.empty) managementNo = aksCandidate;
    }
    if (!managementNo) {
      managementNo = await ensureManagementNo7(firestore);
    }

    // === 2) 顧客特定 ===
    const head100 = String(fullOcrText || bodyPool).slice(0, 100);
    let customer =
      (head100 && (await detectCustomer(firestore, head100))) ||
      (await detectCustomer(firestore, bodyPool));

    // === 2.5) メインPDF + サムネ（300px） ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null;

    if (bucket) {
      if (isFax) {
        // FAX → PDF添付をそのまま使用
        const pdfAttachments = (attachments || []).filter(
          (p) => typeof p === "string" && p.toLowerCase().endsWith(".pdf")
        );
        if (pdfAttachments.length > 0) {
          mainPdfPath = pdfAttachments[0];
          try {
            mainPdfThumbnailPath = await renderPdfToThumbnail({
              bucket,
              pdfGsUri: pdfAttachments[0],
              messageId,
            });
          } catch (e) {
            console.warn("renderPdfToThumbnail failed:", e);
          }
        }
      } else {
        // mail → HTML → PDF → サムネ(300px)
        const htmlSource =
          data.textHtml ||
          (data.textPlain
            ? `<pre>${escapeHtml(String(data.textPlain))}</pre>`
            : null);
        if (htmlSource) {
          try {
            const { pdfPath, thumbnailPath } =
              await renderMailHtmlToPdfAndThumbnail({
                bucket,
                messageId,
                html: htmlSource,
              });
            mainPdfPath = pdfPath;
            mainPdfThumbnailPath = thumbnailPath;
          } catch (e) {
            console.warn("renderMailHtmlToPdfAndThumbnail failed:", e);
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
          rotation: firstPageRotation || 0,
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
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} rot=${firstPageRotation}`
    );
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/* ================= 管理番号 ================= */

export function extractAksNo7(text = "") {
  const re = /AKSNO\s*[:：]?\s*([A-Za-z0-9]{7})/i;
  const m = text.match(re);
  return m ? m[1].toUpperCase() : null;
}

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

function angleDeg(p1 = {}, p2 = {}) {
  const dx = (p2.x ?? 0) - (p1.x ?? 0);
  const dy = (p2.y ?? 0) - (p1.y ?? 0);
  const rad = Math.atan2(dy, dx);
  let deg = (rad * 180) / Math.PI;
  if (deg < 0) deg += 360;
  return deg;
}

function quantizeRotation(deg) {
  const c = [0, 90, 180, 270];
  let best = 0;
  let bestDiff = 99999;
  for (const x of c) {
    const d = Math.min(Math.abs(deg - x), 360 - Math.abs(deg - x));
    if (d < bestDiff) {
      bestDiff = d;
      best = x;
    }
  }
  return best;
}

function estimatePageRotationFromBlocks(page) {
  const votes = { 0: 0, 90: 0, 180: 0, 270: 0 };
  const blocks = page?.blocks || [];

  for (const b of blocks) {
    const v = b.boundingBox?.vertices || [];
    if (v.length >= 2) {
      const deg = angleDeg(v[0], v[1]);
      votes[quantizeRotation(deg)]++;
    }
  }

  return Number(Object.entries(votes).sort((a, b) => b[1] - a[1])[0]?.[0] ?? 0);
}

async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "", rotation: 0 };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random().toString(36).slice(2)}/`;
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

  const [files] = await storage.bucket(info.bucket).getFiles({ prefix: tmpPrefix });
  if (!files.length) return { text: "", rotation: 0 };

  const [buf] = await files[0].download();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const resp = json.responses?.[0];
  const text = resp?.fullTextAnnotation?.text || "";
  const page = resp?.fullTextAnnotation?.pages?.[0];

  return {
    text,
    rotation: page ? estimatePageRotationFromBlocks(page) : 0,
  };
}

async function ocrImageTextWithRotation(gcsUri) {
  const [res] = await client.documentTextDetection(gcsUri);
  const text =
    res.fullTextAnnotation?.text ||
    res.textAnnotations?.[0]?.description ||
    "";
  const page = res.fullTextAnnotation?.pages?.[0];
  return {
    text,
    rotation: page ? estimatePageRotationFromBlocks(page) : 0,
  };
}

async function runOcr(attachments) {
  let fullOcrText = "";
  let firstPageRotation = null;

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
        r = await ocrImageTextWithRotation(uri);
      }

      if (r?.text) {
        fullOcrText += (fullOcrText ? "\n" : "") + r.text;
        if (firstPageRotation == null) firstPageRotation = r.rotation;
      }
    } catch (e) {
      console.warn("OCR failed:", uri, e);
    }
  }

  return {
    fullOcrText,
    firstPageRotation: firstPageRotation ?? 0,
  };
}

/* ================= 顧客特定 ================= */

async function detectCustomer(firestore, sourceText) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const arr = snap.data()?.customers || [];
    const s = (sourceText || "").replace(/\s+/g, "").toLowerCase();

    for (const c of arr) {
      const aliases = (c.aliases || []).map((a) => String(a).toLowerCase());
      if (
        aliases.some((a) =>
          s.includes(a.replace(/\s+/g, ""))
        )
      ) {
        return { id: c.id, name: c.name };
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/* ================= GCS URI parse ================= */

function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

/* ================= HTML→PDF + 300px サムネ ================= */

async function renderMailHtmlToPdfAndThumbnail({ bucket, messageId, html }) {
  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;
  const jpgTmp = `/tmp/mail-${safeId}.jpg`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });

    // PDF作成
    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
    });

    // サムネ生成（幅300px）
    await page.setViewport({ width: 1024, height: 1400 });
    const zoom = 300 / 1024;

    await page.evaluate((zoom) => {
      document.body.style.zoom = zoom;
    }, zoom);

    await page.screenshot({
      path: jpgTmp,
      type: "jpeg",
      quality: 80,
      fullPage: true,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  // GCS アップロード
  const pdfObjectPath = `mail_rendered/${safeId}.pdf`;
  const jpgObjectPath = `mail_thumbs/${safeId}.jpg`;

  const pdfBuf = await fs.readFile(pdfTmp);
  const jpgBuf = await fs.readFile(jpgTmp);

  await bucket.file(pdfObjectPath).save(pdfBuf, { resumable: false, contentType: "application/pdf" });
  await bucket.file(jpgObjectPath).save(jpgBuf, { resumable: false, contentType: "image/jpeg" });

  return {
    pdfPath: `gs://${bucket.name}/${pdfObjectPath}`,
    thumbnailPath: `gs://${bucket.name}/${jpgObjectPath}`,
  };
}

/* ================= PDF→300px サムネ ================= */

async function renderPdfToThumbnail({ bucket, pdfGsUri, messageId }) {
  const info = parseGsUri(pdfGsUri);
  const safeId = sanitizeId(messageId);

  const localPdf = `/tmp/fax-${safeId}.pdf`;
  const localJpg = `/tmp/fax-${safeId}.jpg`;

  // PDF ダウンロード
  await storage.bucket(info.bucket).file(info.path).download({ destination: localPdf });

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();

    await page.goto(`file://${localPdf}`, { waitUntil: "networkidle0" });

    // サムネ生成（幅300px）
    await page.setViewport({ width: 1024, height: 1400 });
    const zoom = 300 / 1024;

    await page.evaluate((zoom) => {
      document.body.style.zoom = zoom;
    }, zoom);

    await page.screenshot({
      path: localJpg,
      type: "jpeg",
      quality: 80,
      fullPage: true,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  const thumbObjectPath = `fax_thumbs/${safeId}.jpg`;
  const jpgBuf = await fs.readFile(localJpg);

  await bucket.file(thumbObjectPath).save(jpgBuf, {
    resumable: false,
    contentType: "image/jpeg",
  });

  return `gs://${bucket.name}/${thumbObjectPath}`;
}

/* ================= Helper ================= */

function sanitizeId(id) {
  return String(id || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 100);
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
