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
 * - 管理番号：既存抽出なければ 6桁HEX でグローバル増分
 * - 図面判定：ゆるめヒューリスティクス（寸法/Φ/板厚/図面/縮尺/レーザ/曲/まげ）
 * - 顧客特定：system_configs/system_customers_master を走査（aliases 含む単純含有チェック）
 * - Firestore: managementNo / customer / 工程TODO / ocr.fullText / ocr.topText を保存
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];

    // === 0) OCR ===
    const { fullOcrText, topOcrText, hasDrawing } = await runOcrAndDetectDrawing(
      attachments
    );

    // メール本文は index.js 側で保存済み。ここでは OCR結果を統合。
    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号 ===
    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    // === 2) 顧客特定 ===
    let customer = null;

    // (A) OCRの1行目優先
    if (topOcrText) {
      customer = await detectCustomer(firestore, topOcrText);
    }
    // (B) 全文フォールバック
    if (!customer) {
      customer = await detectCustomer(firestore, textPool);
    }

    // === 3) 図面・工程TODO ===
    const process = detectProcessTodoByText(fullOcrText);

    // === 4) Firestore反映 ===
    await msgRef.set(
      {
        managementNo,
        parentManagementNo: managementNo,
        customerId: customer?.id || null,
        customerName: customer?.name || null,
        ...process,
        ocr: {
          fullText: fullOcrText || "",
          topText: topOcrText || "",
          hasDrawing: !!process.hasDrawing,
          at: Date.now(),
        },
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} drawing=${process.hasDrawing}`
    );
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/** ================= 管理番号 ================= */
export function extractManagementNo(text = "") {
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
    const max = Math.pow(16, digits);
    let newDigits = digits;

    if (next >= max) {
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

/** ================ OCRユーティリティ ================= */
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

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

  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const first = json.responses?.[0];
  return first?.fullTextAnnotation?.text || "";
}

async function ocrImageText(gcsUri) {
  const [res] = await client.textDetection(gcsUri);
  return res.fullTextAnnotation?.text || res.textAnnotations?.[0]?.description || "";
}

// === 添付1枚目だけOCR実行 ===
async function runOcrAndDetectDrawing(attachments) {
  if (!attachments?.length) return { fullOcrText: "", topOcrText: "", hasDrawing: false };

  const uri = attachments.find((x) => x?.startsWith("gs://"));
  if (!uri) return { fullOcrText: "", topOcrText: "", hasDrawing: false };

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
  }

  const topOcrText = text.split("\n")[0] || "";
  const hasDrawing = detectProcessTodoByText(text).hasDrawing;
  return { fullOcrText: text, topOcrText, hasDrawing };
}

/** ================ 顧客マスタ照合 ================= */
async function detectCustomer(firestore, sourceText) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const arr = Object.values(snap.data() || {});
    const searchSource = (sourceText || "").toLowerCase();

    for (const c of arr) {
      const aliases = Array.isArray(c.aliases) ? c.aliases : [];
      for (const alias of aliases) {
        if (!alias) continue;
        if (searchSource.includes(String(alias).toLowerCase())) {
          console.log(`✅ Customer matched: ${alias} -> ${c.name}`);
          return { id: c.id, name: c.name };
        }
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ================ 図面・工程判定 ================= */
function detectProcessTodoByText(fullText = "") {
  const t = fullText.toLowerCase();
  const hasLaserOrBend = /(レーザ|曲|まげ|φ)/i.test(t);

  if (hasLaserOrBend) {
    return {
      hasDrawing: true,
      processStatusLaser: "todo",
      processStatusBending: "todo",
      processStatusSeichaku: "none",
      processStatusShear: "none",
    };
  } else {
    return {
      hasDrawing: false,
      processStatusLaser: "none",
      processStatusBending: "none",
      processStatusSeichaku: "todo",
      processStatusShear: "todo",
    };
  }
}
