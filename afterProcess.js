// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import puppeteer from "puppeteer";
import fs from "fs/promises";
import { mirrorMessageToSupabase } from "./supabaseSync.js";

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

/** ================= MAIN ENTRY =================
 * index.js の saveMessageDoc() 直後に呼ばれる想定
 * runAfterProcess({ messageId, firestore, bucket })
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];
    const isFax = data.messageType === "fax";

    // === 0) OCR（PDF/TIFF/画像）→ PDF/TIFFは1ページ目のみ + 回転角推定 ===
    const { fullOcrText, firstPageRotation } = await runOcr(attachments);

    // === 本文ソース（subject除外） ===
    const bodyPool = [
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号決定ロジック（AKSNO優先 / 7桁固定） ===
    const aksCandidate = extractAksNo7(bodyPool);
    let managementNo = null;

    if (aksCandidate) {
      const existingSnap = await firestore
        .collection("messages")
        .where("managementNo", "==", aksCandidate)
        .limit(1)
        .get();

      if (!existingSnap.empty) {
        managementNo = aksCandidate;
      }
    }

    if (!managementNo) {
      managementNo = await ensureManagementNo7(firestore);
    }

    // === 2) 顧客特定 ===
    const head100 = String(fullOcrText || bodyPool).slice(0, 100);
    let customer = null;
    if (head100) customer = await detectCustomer(firestore, head100);
    if (!customer) customer = await detectCustomer(firestore, bodyPool);

    // === 2.5) メインPDF + サムネ決定（mail / fax 共通のUI用） ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null;

    if (bucket) {
      if (isFax) {
        // 添付PDFをそのままメイン扱い
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
            console.warn("renderPdfToThumbnail failed:", e?.message || e);
          }
        }
      } else {
        // mail: HTML → PDF → サムネ
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
            console.warn(
              "renderMailHtmlToPdfAndThumbnail failed:",
              e?.message || e
            );
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

    // === 4) Supabase にミラー（別ファイル） ===
    // Firestore に保存した情報 + 計算結果を渡しておく
    const dataForMirror = {
      ...data,
      managementNo,
      customerId: customer?.id ?? data.customerId ?? null,
      customerName: customer?.name ?? data.customerName ?? null,
      ocr: {
        ...(data.ocr || {}),
        fullText: fullOcrText || "",
        rotation: firstPageRotation || 0,
      },
      mainPdfPath,
      mainPdfThumbnailPath,
    };

    mirrorMessageToSupabase({
      messageId,
      data: dataForMirror,
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

/** ================= 管理番号（7桁固定 & AKSNO抽出） ================= */
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
    const digits = FIXED_DIGITS;

    let next = current + 1;
    const max = Math.pow(16, digits);
    if (next >= max) next = 1;

    tx.set(
      seqRef,
      { v: next, digits, updatedAt: Date.now() },
      { merge: true }
    );

    return next.toString(16).toUpperCase().padStart(digits, "0");
  });
}

/** ================ OCRユーティリティ（回転角推定つき） ================= */

/** 2点の角度（度）0..360 */
function angleDeg(p1 = {}, p2 = {}) {
  const dx = (p2.x ?? 0) - (p1.x ?? 0);
  const dy = (p2.y ?? 0) - (p1.y ?? 0);
  const rad = Math.atan2(dy, dx);
  let deg = (rad * 180) / Math.PI;
  if (deg < 0) deg += 360;
  return deg;
}

/** 0/90/180/270 に丸める */
function quantizeRotation(deg) {
  const candidates = [0, 90, 180, 270];
  let best = 0;
  let bestDiff = 1e9;
  for (const c of candidates) {
    const d = Math.min(Math.abs(deg - c), 360 - Math.abs(deg - c));
    if (d < bestDiff) {
      bestDiff = d;
      best = c;
    }
  }
  return best;
}

/** ページのブロック上辺角度の多数決で回転角を推定 */
function estimatePageRotationFromBlocks(page) {
  const votes = { 0: 0, 90: 0, 180: 0, 270: 0 };
  const blocks = page?.blocks || [];
  for (const b of blocks) {
    const v = b.boundingBox?.vertices || [];
    if (v.length >= 2) {
      const deg = angleDeg(v[0], v[1]);
      const q = quantizeRotation(deg);
      votes[q] = (votes[q] || 0) + 1;
    }
  }
  const entries = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  return Number(entries[0]?.[0] ?? 0);
}

// PDF/TIFF → 1ページ目 OCR
async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "", rotation: 0 };

  const tmpPrefix = `_vision_out/${Date.now()}_${Math.random()
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
  if (!files.length) return { text: "", rotation: 0 };

  const [buf] = await files[0].download();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const resp = json.responses?.[0];
  const text = resp?.fullTextAnnotation?.text || "";

  const page = resp?.fullTextAnnotation?.pages?.[0];
  const rotation = page ? estimatePageRotationFromBlocks(page) : 0;

  return { text, rotation };
}

// 画像 OCR（documentTextDetection）
async function ocrImageTextWithRotation(gcsUri) {
  const [res] = await client.documentTextDetection(gcsUri);
  const text =
    res.fullTextAnnotation?.text ||
    res.textAnnotations?.[0]?.description ||
    "";
  const page = res.fullTextAnnotation?.pages?.[0];
  const rotation = page ? estimatePageRotationFromBlocks(page) : 0;
  return { text, rotation };
}

// 全添付の OCR
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

      if (!r?.text) continue;

      fullOcrText += (fullOcrText ? "\n" : "") + r.text;

      if (firstPageRotation == null) {
        firstPageRotation = r.rotation ?? 0;
      }
    } catch (e) {
      console.warn("OCR failed:", uri, e?.message || e);
    }
  }

  return { fullOcrText, firstPageRotation: firstPageRotation ?? 0 };
}

/** ================ 顧客マスタ照合 ================= */
async function detectCustomer(firestore, sourceText) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const arr = snap.data()?.customers || [];
    const searchSource = (sourceText || "")
      .replace(/\s+/g, "")
      .toLowerCase();

    for (const c of arr) {
      const aliases = (c.aliases || []).map((a) => String(a).toLowerCase());
      if (
        aliases.some((a) => {
          const cleanedAlias = a.replace(/\s+/g, "");
          return cleanedAlias && searchSource.includes(cleanedAlias);
        })
      ) {
        return { id: c.id, name: c.name };
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ================= GCS URI パース ================= */
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

/** ================= メールHTML → PDF + サムネ生成 ================= */
async function renderMailHtmlToPdfAndThumbnail({ bucket, messageId, html }) {
  if (!bucket) {
    throw new Error("Bucket is required for renderMailHtmlToPdfAndThumbnail");
  }

  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;
  const jpgTmp = `/tmp/mail-${safeId}.jpg`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });

    // PDF 出力
    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
    });

    // サムネ（画面キャプチャ）
    await page.setViewport({ width: 1024, height: 1400 });
    await page.screenshot({
      path: jpgTmp,
      type: "jpeg",
      quality: 80,
      fullPage: true,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  const pdfObjectPath = `mail_rendered/${safeId}.pdf`;
  const jpgObjectPath = `mail_thumbs/${safeId}.jpg`;

  const pdfBuf = await fs.readFile(pdfTmp);
  const jpgBuf = await fs.readFile(jpgTmp);

  await bucket.file(pdfObjectPath).save(pdfBuf, {
    resumable: false,
    contentType: "application/pdf",
  });
  await bucket.file(jpgObjectPath).save(jpgBuf, {
    resumable: false,
    contentType: "image/jpeg",
  });

  return {
    pdfPath: `gs://${bucket.name}/${pdfObjectPath}`,
    thumbnailPath: `gs://${bucket.name}/${jpgObjectPath}`,
  };
}

/** ================= FAX PDF → サムネ生成 ================= */
async function renderPdfToThumbnail({ bucket, pdfGsUri, messageId }) {
  if (!bucket) {
    throw new Error("Bucket is required for renderPdfToThumbnail");
  }
  const info = parseGsUri(pdfGsUri);
  if (!info) throw new Error("Invalid GCS URI: " + pdfGsUri);

  const safeId = sanitizeId(messageId);
  const localPdf = `/tmp/fax-${safeId}.pdf`;
  const localJpg = `/tmp/fax-${safeId}.jpg`;

  // 元PDFをダウンロード
  const srcBucket = storage.bucket(info.bucket);
  await srcBucket.file(info.path).download({ destination: localPdf });

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  try {
    const page = await browser.newPage();
    await page.goto(`file://${localPdf}`, { waitUntil: "networkidle0" });
    await page.setViewport({ width: 1024, height: 1400 });
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

/** ================= ヘルパ ================= */
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
