import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

/** ================= MAIN ENTRY =================
 * index.js の saveMessageDoc() 直後に呼ばれる想定
 */
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];

    // === 0) OCR（PDF/TIFF/画像）→ PDF/TIFFは1ページ目のみ ===
    const fullOcrText = await runOcr(attachments);

    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（既存抽出 or 自動採番） ===
    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    // === 2) 顧客特定（冒頭100文字 → 全文フォールバック） ===
    const head100 = String(fullOcrText || textPool).slice(0, 100);
    let customer = null;
    if (head100) customer = await detectCustomer(firestore, head100);
    if (!customer) customer = await detectCustomer(firestore, textPool);

    // === 3) 工程TODOは行わない（すべて "none" に固定） ===
    const processFixed = {
      processStatusLaser: "none",
      processStatusBending: "none",
      processStatusSeichaku: "none",
      processStatusShear: "none",
    };

    // === 4) 添付ごとに GCS V4 署名URL（7日）を発行 ===
    const signedUrls = [];
    const expiresAt = Date.now() + 7 * 24 * 60 * 60 * 1000; // 7日
    for (const gsUri of attachments) {
      if (!gsUri?.startsWith("gs://")) continue;
      try {
        const url = await getSignedUrlForGsUri(gsUri, expiresAt);
        signedUrls.push({ gsUri, signedUrl: url, expiresAt });
      } catch (e) {
        console.warn("signedUrl generation failed:", gsUri, e?.message || e);
      }
    }

    // === 5) Firestore 反映 ===
    await msgRef.set(
      {
        managementNo,
        parentManagementNo: managementNo,
        customerId: customer?.id || null,
        customerName: customer?.name || null,
        ...processFixed,
        ocr: {
          fullText: fullOcrText || "",
          at: Date.now(),
        },
        signedUrls, // ← ここに署名URL一覧を保存
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} signed=${signedUrls.length}`
    );
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/** ================= 署名URLユーティリティ ================= */
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

async function getSignedUrlForGsUri(gsUri, expiresAtMs) {
  const info = parseGsUri(gsUri);
  if (!info) throw new Error("invalid gs:// uri");
  const [url] = await storage
    .bucket(info.bucket)
    .file(info.path)
    .getSignedUrl({
      version: "v4",
      action: "read",
      expires: new Date(expiresAtMs), // Date or ms epoch
      // contentType / contentDisposition は必要に応じて GCS 側メタに設定を推奨
    });
  return url;
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
    const digits =
      snap.exists && snap.data()?.digits ? Number(snap.data().digits) : 6;

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

// PDF/TIFF を 1ページ目だけ OCR（非同期バッチ）
async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return "";
  const tmpPrefix = `_vision_out/${Date.now()}_${Math.random()
    .toString(36)
    .slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const [op] = await client.asyncBatchAnnotateFiles({
    requests: [
      {
        inputConfig: { gcsSource: { uri: gcsUri }, mimeType }, // ← 可変
        features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
        outputConfig: { gcsDestination: { uri: outUri }, batchSize: 1 },
        pages: [1], // 1ページ目のみ
      },
    ],
  });

  await op.promise();

  const [files] = await storage
    .bucket(info.bucket)
    .getFiles({ prefix: tmpPrefix });
  if (!files.length) return "";

  const [buf] = await files[0].download();
  // 後始末（出力JSON掃除）
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const first = json.responses?.[0];
  return first?.fullTextAnnotation?.text || "";
}

async function ocrImageText(gcsUri) {
  const [res] = await client.textDetection(gcsUri);
  return (
    res.fullTextAnnotation?.text || res.textAnnotations?.[0]?.description || ""
  );
}

// 図面判定なし・工程判定なし・1ページOCRのみ
async function runOcr(attachments) {
  let fullOcrText = "";

  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

    let text = "";
    try {
      if (lower.endsWith(".pdf")) {
        text = await ocrFirstPageFromFile(uri, "application/pdf");
      } else if (lower.endsWith(".tif") || lower.endsWith(".tiff")) {
        text = await ocrFirstPageFromFile(uri, "image/tiff");
      } else {
        text = await ocrImageText(uri);
      }
    } catch (e) {
      console.warn("OCR failed:", uri, e?.message || e);
      continue;
    }
    if (!text) continue;

    fullOcrText += (fullOcrText ? "\n" : "") + text;
  }

  return fullOcrText;
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
    const searchSource = (sourceText || "").replace(/\s+/g, "").toLowerCase();

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
