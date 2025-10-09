import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";

const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];

    const { fullOcrText, topOcrText } = await runOcrAndDetectDrawing(attachments);

    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    let customer = null;

    // (A) OCRの1行目優先
    if (topOcrText) {
      customer = await detectCustomer(firestore, topOcrText);
    }
    // (B) 全文フォールバック
    if (!customer) {
      customer = await detectCustomer(firestore, textPool);
    }

    // (C) 工程TODO判定（レーザ・まげ・曲）
    const process = detectProcessTodo(fullOcrText);

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
          at: Date.now(),
        },
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} customer=${customer?.name || "-"}`
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

async function runOcrAndDetectDrawing(attachments) {
  let fullOcrText = "";
  let topOcrText = "";

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
    const top = text.split("\n")[0] || "";
    topOcrText += (topOcrText ? " " : "") + top;
  }

  return { fullOcrText, topOcrText };
}

/** ================ 顧客マスタ照合 ================= */
function coerceAliases(val) {
  if (!val) return [];
  if (Array.isArray(val)) {
    return val.map((s) => String(s ?? "").trim()).filter((s) => s.length > 0);
  }
  if (typeof val === "string") {
    return val
      .split(/[,\u3001\uFF0C，、｡]/g)
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
  }
  return [];
}

async function detectCustomer(firestore, sourceText) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const data = snap.data() || {};
    const arr = Array.isArray(data) ? data : Object.values(data);

    const src = String(sourceText || "").toLowerCase();

    for (const c of arr) {
      const aliases = coerceAliases(c?.aliases).map((a) => a.toLowerCase());
      if (aliases.length === 0) continue;

      if (aliases.some((a) => a && src.includes(a))) {
        console.log(`✅ Customer matched by alias: [${aliases.join(" | ")}] -> ${c.name}`);
        return { id: c.id ?? null, name: c.name ?? null };
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ================ 工程TODO判定 ================= */
function detectProcessTodo(text = "") {
  const lower = text.toLowerCase();
  if (/(レーザ|曲|まげ)/.test(lower)) {
    return {
      processStatusLaser: "todo",
      processStatusBending: "todo",
      processStatusSeichaku: "none",
      processStatusShear: "none",
    };
  } else {
    return {
      processStatusLaser: "none",
      processStatusBending: "none",
      processStatusSeichaku: "todo",
      processStatusShear: "todo",
    };
  }
}
