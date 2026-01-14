// afterProcess.js
import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
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

    // === 0) OCR（テキストだけ取得、rotation は廃止）===
    const { fullOcrText } = await runOcr(attachments);

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

    // === 2.5) メインPDF（サムネは一旦なし） ===
    let mainPdfPath = null;
    let mainPdfThumbnailPath = null;

    // FAX のときだけ、メインPDFとして「最初の添付」を覚えておく
    if (bucket && isFax) {
      const firstAttachment = (attachments || []).find(
        (p) => typeof p === "string"
      );
      if (firstAttachment) {
        mainPdfPath = firstAttachment;
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
