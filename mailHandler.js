// faxHandler.js
import { bucket } from "./index.js";

// ==== 便利関数（このファイル内に内蔵） ====
function getHeader(headers, name) {
  const h = (headers || []).find(
    (x) => x.name?.toLowerCase() === name.toLowerCase()
  );
  return h?.value || "";
}

function flattenParts(parts) {
  const out = [];
  const stack = [...(parts || [])];
  while (stack.length) {
    const p = stack.shift();
    out.push(p);
    if (p?.parts?.length) stack.push(...p.parts);
  }
  return out;
}

function extractBodies(payload) {
  let textPlain = "";
  let textHtml = "";
  if (!payload) return { textPlain, textHtml };
  const parts = payload.parts ? flattenParts(payload.parts) : [payload];
  for (const p of parts) {
    if (p.mimeType === "text/plain" && p.body?.data) {
      textPlain += Buffer.from(
        p.body.data.replace(/-/g, "+").replace(/_/g, "/"),
        "base64"
      ).toString("utf8");
    }
    if (p.mimeType === "text/html" && p.body?.data) {
      textHtml += Buffer.from(
        p.body.data.replace(/-/g, "+").replace(/_/g, "/"),
        "base64"
      ).toString("utf8");
    }
  }
  return { textPlain, textHtml };
}

// 例: doc202510032217370774652265.pdf → 2025-10-03 22:17:37
function parseFaxAtFromFilename(filename) {
  if (!filename) return null;
  const m = filename.match(/^doc(\d{14})/); // YYYYMMDDHHmmss
  if (!m) return null;
  const s = m[1];
  const y = Number(s.slice(0, 4));
  const mo = Number(s.slice(4, 6));
  const d = Number(s.slice(6, 8));
  const h = Number(s.slice(8, 10));
  const mi = Number(s.slice(10, 12));
  const se = Number(s.slice(12, 14));
  // JS Date: month is 0-based; ここではUTCで固める
  return new Date(Date.UTC(y, mo - 1, d, h, mi, se));
}

// 件名や本文からFAX番号らしきものを抽出（暫定）
function parseFaxNumberFromText(...texts) {
  const joined = (texts || []).filter(Boolean).join(" ");
  if (!joined) return null;
  const candidates = joined.match(/\+?\d[\d\-\s()ー―‐－]{6,}\d/g);
  if (!candidates) return null;
  let best = candidates.sort((a, b) => b.length - a.length)[0];
  best = best.replace(/[^\d+]/g, "");
  if (best.replace(/^\+/, "").length < 7) return null;
  return best;
}

// Gmail API から attachmentId を使って添付を取得して保存
async function saveAttachmentToGCSViaAPI({
  userEmail,
  messageId,
  part,
  gmail,
  GCS_BUCKET,
}) {
  if (!bucket) return null;
  const attachId = part?.body?.attachmentId;
  if (!attachId) return null;

  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: attachId,
  });

  const b64 = (res.data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  const bytes = Buffer.from(b64, "base64");
  const filename = part.filename || `attachment-${attachId}`;
  const objectPath = `gmail/${encodeURIComponent(userEmail)}/${messageId}/${filename}`;

  await bucket.file(objectPath).save(bytes, {
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" },
  });

  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ==== FAX専用処理 ====
export async function handleFaxMail(m, payload, emailAddress, db, GCS_BUCKET, gmail) {
  const headers = payload?.headers || [];
  const subject = getHeader(headers, "Subject");
  const from = getHeader(headers, "From");
  const to = getHeader(headers, "To");
  const dateHdr = getHeader(headers, "Date");
  const internalDateMs = m.data.internalDate
    ? Number(m.data.internalDate)
    : Date.parse(dateHdr);

  const { textPlain, textHtml } = extractBodies(payload);

  // 添付保存 & FAX時刻の推定
  const attachments = [];
  let faxAt = null;
  const parts = flattenParts(payload?.parts || []);
  for (const p of parts) {
    if (p?.filename && p.body?.attachmentId) {
      if (!faxAt) {
        const ts = parseFaxAtFromFilename(p.filename);
        if (ts) faxAt = ts;
      }
      const path = await saveAttachmentToGCSViaAPI({
        userEmail: emailAddress,
        messageId: m.data.id,
        part: p,
        gmail,
        GCS_BUCKET,
      });
      if (path) attachments.push(path);
    }
  }

  const faxNumber = parseFaxNumberFromText(subject, textPlain, textHtml);

  // ★ humanReadableId の生成
  let humanReadableId = null;
  if (faxAt) {
    const y  = faxAt.getUTCFullYear();
    const MM = String(faxAt.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(faxAt.getUTCDate()).padStart(2, "0");
    const HH = String(faxAt.getUTCHours()).padStart(2, "0");
    const mm = String(faxAt.getUTCMinutes()).padStart(2, "0");
    humanReadableId = `${y}${MM}${dd}${HH}${mm}`; 
  }
  
  await db.collection("messages").doc(m.data.id).set(
    {
      user: emailAddress,
      messageType: "fax",
      threadId: m.data.threadId,
      internalDate: internalDateMs || null,
      receivedAt: internalDateMs ? new Date(internalDateMs) : new Date(),
      from,
      to,
      subject,
      snippet: m.data.snippet || "",
      textPlain,
      textHtml,
      attachments,
      gcsBucket: GCS_BUCKET || null,
      createdAt: Date.now(),
      faxAt: faxAt || null,
      faxNumber: faxNumber || null,
      
      // ====================================
      // ★追加フィールド（全10項目）
      // ====================================
      
      humanReadableId: humanReadableId || null, 

      customerName: "",        // 得意先名
      deliveryDate: null,      // 納期

      processStatusSeichaku: "", // 定尺
      processStatusShear: "",    // シャー
      processStatusLaser: "",    // レーザー
      processStatusBending: "",  // 曲げ

      deliveryStatus: "",      // 納品
      stockStatus: "",         // 在庫
    },
    { merge: true }
  );

  console.log(
    `[FAX] saved message ${m.data.id} humanReadableId=${humanReadableId || "null"} (attachments: ${attachments.length})`
  );
}
