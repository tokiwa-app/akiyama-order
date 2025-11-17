import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "";

// ====== Vision / Storage clients ======
const client = new vision.ImageAnnotatorClient();
const storage = new Storage();

/** ================= MAIN ENTRY =================
 * index.js の saveMessageDoc() 直後に呼ばれる想定
 */
export async function runAfterProcess({ messageId, firestore }) {
  try {
    const msgRef = firestore.collection("messages").doc(messageId);
    const msgSnap = await msgRef.get();
    if (!msgSnap.exists) return;

    const data = msgSnap.data();
    const attachments = data.attachments || [];

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
        processedAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `✅ afterProcess done id=${messageId} mgmt=${managementNo} rot=${firstPageRotation}`
    );

    // ======================================================
    // === 4) ★ RDB（Cloud Run API）へ同期 POST /actions ★===
    // ======================================================
    try {
      if (!API_BASE_URL) {
        console.warn("NEXT_PUBLIC_API_BASE_URL が未設定のため RDB同期スキップ");
      } else {

        // === 発生日（メールの受信日時） ===
        let occurredAt = new Date();
        const receivedAt = data.receivedAt;

        if (receivedAt && typeof receivedAt.toDate === "function") {
          occurredAt = receivedAt.toDate();
        } else if (receivedAt instanceof Date) {
          occurredAt = receivedAt;
        } else if (data.internalDate) {
          occurredAt = new Date(data.internalDate);
        }

        // === 必須項目＋最低構成の同期内容 ===
        const payload = {
          tenantId: "AKIYAMA",
          action: {
            action_type: "WORK",                  // ★ 必須（大文字）
            occurred_at: occurredAt.toISOString(),// ★ 必須
            status: "NEW",                        // ★ 必須

            external_ref_id: messageId,           // Firestore ID（同期キー）
            partner_id: customer?.id || null,     // 顧客ID（なくてもOK）
            main_target_id: managementNo,         // 管理番号（任意だが付けておく）
            main_target_name: customer?.name || null, // 名前（任意）
          },
          lines: [],
        };

        // POST /actions
        const res = await fetch(`${API_BASE_URL}/actions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        if (!res.ok) {
          const text = await res.text().catch(() => "");
          console.error("❌ RDB同期失敗:", res.status, text);
        } else {
          console.log("✅ RDB同期 OK");
        }
      }
    } catch (e) {
      console.error("RDB同期中エラー:", e);
    }
    // ======================================================

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

    tx.set(seqRef, { v: next, digits, updatedAt: Date.now() }, { merge: true });

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
    res.fullTextAnnotation?.text || res.textAnnotations?.[0]?.description || "";
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
      continue;
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

/** ================= GCS URI パース ================= */
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}
