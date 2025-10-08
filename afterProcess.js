// afterProcess.js
import vision from "@google-cloud/vision";
import { FieldValue } from "@google-cloud/firestore";

const client = new vision.ImageAnnotatorClient();

/** ========== MAIN ENTRY ========== **/
export async function runAfterProcess({ messageId, firestore, bucket }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];
    const textPool = [
      data.subject || "",
      data.textPlain || "",
      data.textHtml || "",
    ].join(" ");

    // --- 1. 管理番号抽出または生成 ---
    const existing = extractManagementNo(textPool);
    const managementNo = await ensureManagementNo(firestore, existing);

    // --- 2. OCRから図面有無を確認 ---
    const hasDrawing = await detectDrawingFromAttachments(bucket, attachments);

    // --- 3. 顧客特定 ---
    const customer = await detectCustomer(firestore, textPool);

    // --- 4. 工程Todo設定 ---
    const process = detectProcessTodo(hasDrawing);

    // --- 5. Firestore更新 ---
    await msgRef.set(
      {
        managementNo,
        parentManagementNo: managementNo, // 同一案件グループ
        customerId: customer?.id || null,
        customerName: customer?.name || null,
        ...process,
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `✅ afterProcess completed for ${messageId} (管理No=${managementNo})`
    );
  } catch (e) {
    console.error("afterProcess error:", e);
  }
}

/** ========== 管理番号関連 ========== **/
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
    const current = snap.exists && snap.data()?.v ? snap.data().v : 0;
    const digits = snap.exists && snap.data()?.digits ? snap.data().digits : 6;
    let next = current + 1;

    // 上限 FFFFFF を超えたら桁を1増やす
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

    const code = next.toString(16).toUpperCase().padStart(newDigits, "0");
    return code;
  });
}

/** ========== OCR / 図面検出 ========== **/
async function detectDrawingFromAttachments(bucket, attachments) {
  if (!attachments?.length) return false;

  for (const gcsPath of attachments) {
    if (!gcsPath.startsWith("gs://")) continue;
    try {
      const [result] = await client.textDetection(gcsPath);
      const text = result.fullTextAnnotation?.text || "";
      if (isDrawingLayout(text)) return true;
    } catch (e) {
      console.warn("OCR failed:", e.message);
    }
  }
  return false;
}

function isDrawingLayout(text) {
  // 上部1cm（全体の前半部）を想定して「図面・A3・A4・寸法・Φ・t」などが出たら図面と判断
  const top = text.split("\n").slice(0, 10).join(" ");
  return /図面|A3|A4|寸法|Φ|φ|t\d|DRW|DXF/i.test(top);
}

/** ========== 顧客マスタ照合 ========== **/
async function detectCustomer(firestore, textPool) {
  try {
    const snap = await firestore
      .collection("system_configs")
      .doc("system_customers_master")
      .get();
    if (!snap.exists) return null;

    const arr = Object.values(snap.data() || {});
    const text = textPool.replace(/\s+/g, "").toLowerCase();

    for (const c of arr) {
      const name = (c.name || "").replace(/\s+/g, "").toLowerCase();
      const kana = (c.kana || "").toLowerCase();
      const short = (c.shortName || "").toLowerCase();
      const aliases = (c.aliases || []).map((a) => a.toLowerCase());
      if (
        text.includes(name) ||
        text.includes(kana) ||
        text.includes(short) ||
        aliases.some((a) => text.includes(a))
      ) {
        return { id: c.id, name: c.name };
      }
    }
  } catch (e) {
    console.error("detectCustomer error:", e);
  }
  return null;
}

/** ========== 工程Todo判定 ========== **/
function detectProcessTodo(hasDrawing) {
  if (hasDrawing) {
    // 図面あり → レーザー or 曲げ
    return {
      processStatusLaser: "todo",
      processStatusBending: "todo",
      processStatusSeichaku: "none",
      processStatusShear: "none",
    };
  } else {
    // 図面なし → 定尺 or シャー
    return {
      processStatusLaser: "none",
      processStatusBending: "none",
      processStatusSeichaku: "todo",
      processStatusShear: "todo",
    };
  }
}
