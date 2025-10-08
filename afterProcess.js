// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

/** ================= MAIN ENTRY =================
 * index.js の saveMessageDoc() 直後に呼ばれる想定
 * - PDF/TIFF は 非同期OCR（asyncBatchAnnotateFiles）
 * - 画像は 同期OCR（textDetection）
 * - 管理番号：既存抽出なければ 6桁HEX でグローバル増分（FFFFFF超えたら7桁）
 * - 図面判定：ゆるめヒューリスティクス（寸法/Φ/板厚/図面/縮尺 等）
 * - 顧客特定：system_configs/system_customers_master を走査（name/kana/shortName/aliases）
 * - Firestore: managementNo / customer / 工程TODO / ocr.fullText / ocr.topText を保存
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];

    // === 0) OCR（PDF/TIFF/画像に対応） ===
    const { fullOcrText, topOcrText, hasDrawing } = await runOcrAndDetectDrawing(
      attachments
    );

    // メール本文は index.js 側で既に保存済みなのでそのまま維持
    // ここでは OCR で得た全文も照合用に含める
    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（既存抽出 or 自動採番） ===
    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    // === 2) 顧客特定（OCR＋本文で照合） ===
    const customer = await detectCustomer(firestore, textPool);

    // === 3) 工程TODO（図面有無） ===
    const process = detectProcessTodo(hasDrawing);

    // === 4) Firestore 反映 ===
    await msgRef.set(
      {
        managementNo,
        parentManagementNo: managementNo,
        customerId: customer?.id || null,
        customerName: customer?.name || null,
        ...process,
        // ★ OCR結果をそのまま保存（注意：1MB/ドキュメント制限）
        ocr: {
          fullText: fullOcrText || "",
          topText: topOcrText || "",
          hasDrawing: !!hasDrawing,
          at: Date.now(),
        },
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(`✅ afterProcess done id=${messageId} mgmt=${managementNo} drawing=${hasDrawing}`);
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/** ================= 管理番号 ================= */
export function extractManagementNo(text = "") {
  // 「管理No / 管理番号 / aksNo」の後ろに英数3〜10桁
  const re = /(?:管理\s*No|管理番号|aksNo)\s*[:：]?\s*([A-Za-z0-9]{3,10})/i;
  const m = text.match(re);
  return m ? m[1].toUpperCase() : null;
}

export async function ensureManagementNo(firestore, extracted) {
  if (extracted) return extracted;

  const seqRef = firestore
    .collection("system_configs")
    .doc("sequence_managementNo_global");

  return await firestore.runTransaction(async (tx) => {
    const snap = await tx.get(seqRef);
    const current = snap.exists && snap.data()?.v ? Number(snap.data().v) : 0;
    const digits = snap.exists && snap.data()?.digits ? Number(snap.data().digits) : 6;

    let next = current + 1;
    const max = Math.pow(16, digits); // 16^digits
    let newDigits = digits;

    if (next >= max) {
      // FFFFFF 到達で 7桁へ拡張
      next = 1;
      newDigits = digits + 1;
    }

    tx.set(
      seqRef,
      { v: next, digits: newDigits, updatedAt: Date.now() },
      { merge: true }
    );

    return next.toString(16).toUpperCase().padStart(newDigits, "0");
  });
}

/** ================ OCRユーティリティ（PDF/TIFF/画像） ================= */
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

// PDF/TIFF → 非同期OCR（1ページ目で十分なら batchSize:1）
async function ocrPdfFirstPageText(gcsUri) {
  const info = parseGsUri(gcsUri);
  if (!info) return "";
  const tmpPrefix = `_vision_out/${Date.now()}_${Math.random().toString(36).slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const [op] = await client.asyncBatchAnnotateFiles({
    requests: [
      {
        inputConfig: { gcsSource: { uri: gcsUri }, mimeType: "application/pdf" },
        features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
        outputConfig: { gcsDestination: { uri: outUri }, batchSize: 1 },
      },
    ],
  });

  await op.promise();

  const [files] = await storage.bucket(info.bucket).getFiles({ prefix: tmpPrefix });
  if (!files.length) return "";
  const [buf] = await files[0].download();

  // cleanup（失敗は無視）
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const first = json.responses?.[0];
  return first?.fullTextAnnotation?.text || "";
}

// 画像（PNG/JPG…）→ 同期OCR
async function ocrImageText(gcsUri) {
  const [res] = await client.textDetection(gcsUri);
  return res.fullTextAnnotation?.text || res.textAnnotations?.[0]?.description || "";
}

// 添付すべてに対してOCR実行 → 全文/上部/図面有無
async function runOcrAndDetectDrawing(attachments) {
  let fullOcrText = "";
  let topOcrText = "";
  let hasDrawing = false;

  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;

    const lower = uri.toLowerCase();
    let text = "";
    try {
      if (lower.endsWith(".pdf") || lower.endsWith(".tif") || lower.endsWith(".tiff")) {
        text = await ocrPdfFirstPageText(uri);
      } else {
        text = await ocrImageText(uri);
      }
    } catch (e) {
      console.warn("OCR failed:", uri, e?.message || e);
      continue;
    }
    if (!text) continue;

    fullOcrText += (fullOcrText ? "\n" : "") + text;
    const top = text.split("\n").slice(0, 40).join(" ");
    topOcrText += (topOcrText ? " " : "") + top;

    if (isDrawingByHeuristics(text)) hasDrawing = true;
  }

  return { fullOcrText, topOcrText, hasDrawing };
}

/** ================ 図面判定（ゆるめ） ================= */
function isDrawingByHeuristics(text) {
  const top = text.split("\n").slice(0, 60).join(" ");
  let score = 0;
  if (/(図面|図番|外形|寸法|公差|材質|板厚|展開|曲げ|穴径|仕上|溶接)/i.test(top)) score++;
  if (/[φΦØ]/.test(top)) score++;
  if (/\bR\s*\d+/.test(top)) score++;
  if (/\bt\s*\d+(\.\d+)?\b/i.test(top)) score++;
  if (/\bA3\b|\bA4\b|\bSCALE\b|縮尺/i.test(top)) score++;
  if (/\b\d+(\.\d+)?\s*mm\b/i.test(top)) score++;
  return score >= 2; // 2点以上で「図面あり」
}

/** ================ 顧客マスタ照合 ================= */
async function detectCustomer(firestore, textPool) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const arr = Object.values(snap.data() || {});
    const text = (textPool || "").replace(/\s+/g, "").toLowerCase();

    for (const c of arr) {
      const name = (c.name || "").replace(/\s+/g, "").toLowerCase();
      const kana = (c.kana || "").toLowerCase();
      const short = (c.shortName || "").toLowerCase();
      const aliases = (c.aliases || []).map((a) => String(a).toLowerCase());

      if (
        (name && text.includes(name)) ||
        (kana && text.includes(kana)) ||
        (short && text.includes(short)) ||
        (aliases.length && aliases.some((a) => a && text.includes(a)))
      ) {
        return { id: c.id, name: c.name };
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ================ 工程TODO判定 ================= */
function detectProcessTodo(hasDrawing) {
  if (hasDrawing) {
    // 図面あり → レーザー/曲げ を TODO
    return {
      processStatusLaser: "todo",
      processStatusBending: "todo",
      processStatusSeichaku: "none",
      processStatusShear: "none",
    };
  } else {
    // 図面なし → 定尺/シャー を TODO
    return {
      processStatusLaser: "none",
      processStatusBending: "none",
      processStatusSeichaku: "todo",
      processStatusShear: "todo",
    };
  }
}
