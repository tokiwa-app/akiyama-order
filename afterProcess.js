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

    // === 0) OCR（PDF/TIFF/画像に対応） ===
    const { fullOcrText, topOcrText, hasDrawing } = await runOcrAndDetectDrawing(
      attachments
    );

    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
      fullOcrText || "",
    ].join(" ");

    // === 1) 管理番号（既存抽出 or 自動採番） ===
    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    // === 2) 顧客特定（修正版） ===
    let customer = null;

    if (topOcrText) {
      customer = await detectCustomer(firestore, topOcrText);
    }
    if (!customer) {
      customer = await detectCustomer(firestore, textPool);
    }

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
    const top = text.split("\n")[0] || "";
    topOcrText += (topOcrText ? " " : "") + top;
    if (isDrawingByHeuristics(text)) hasDrawing = true;
  }

  return { fullOcrText, topOcrText, hasDrawing };
}

/** ================ 図面判定 ================= */
function isDrawingByHeuristics(text) {
  const top = text.split("\n").slice(0, 60).join(" ");
  let score = 0;
  if (/(図面|図番|外形|寸法|公差|材質|板厚|展開|曲げ|穴径|仕上|溶接)/i.test(top)) score++;
  if (/[φΦØ]/.test(top)) score++;
  if (/\bR\s*\d+/.test(top)) score++;
  if (/\bt\s*\d+(\.\d+)?\b/i.test(top)) score++;
  if (/\bA3\b|\bA4\b|\bSCALE\b|縮尺/i.test(top)) score++;
  if (/\b\d+(\.\d+)?\s*mm\b/i.test(top)) score++;
  return score >= 2;
}

/** ================ 顧客マスタ照合（修正版） ================= */
async function detectCustomer(firestore, sourceText) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) {
      console.warn("⚠ system_customers_master not found");
      return null;
    }

    const data = snap.data() || {};
    let arr = [];
    if (Array.isArray(data)) {
      arr = data;
    } else if (data.customers && typeof data.customers === "object") {
      arr = Object.values(data.customers);
    } else {
      arr = Object.values(data);
    }
    arr = arr.filter((x) => x && typeof x === "object");

    const raw = String(sourceText || "").toLowerCase();
    const compact = raw.replace(/\s+/g, "");

    for (const c of arr) {
      const aliases = Array.isArray(c.aliases)
        ? c.aliases
        : (typeof c.aliases === "string" && c.aliases.trim()
            ? [c.aliases]
            : []);

      if (!aliases.length) continue;

      for (const a of aliases) {
        const aRaw = String(a || "").toLowerCase();
        if (!aRaw) continue;

        const aCompact = aRaw.replace(/\s+/g, "");
        if (raw.includes(aRaw) || compact.includes(aCompact)) {
          console.log("✅ detectCustomer hit:", c.name);
          return { id: c.id ?? null, name: c.name ?? null };
        }
      }
    }
    console.warn("⚠ detectCustomer: no match");
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ================ 工程TODO判定 ================= */
function detectProcessTodo(hasDrawing) {
  if (hasDrawing) {
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
