// mailHandler.js
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

// ==== 通常メール処理 ====
export async function handleNormalMail(m, payload, emailAddress, db, GCS_BUCKET, gmail) {
  const headers = payload?.headers || [];
  const subject = getHeader(headers, "Subject");
  const from = getHeader(headers, "From");
  const to = getHeader(headers, "To");
  const cc = getHeader(headers, "Cc");
  const dateHdr = getHeader(headers, "Date");
  const internalDateMs = m.data.internalDate
    ? Number(m.data.internalDate)
    : Date.parse(dateHdr);

  const { textPlain, textHtml } = extractBodies(payload);

  const attachments = [];
  const parts = flattenParts(payload?.parts || []);
  for (const p of parts) {
    if (p?.filename && p.body?.attachmentId) {
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

  await db.collection("messages").doc(m.data.id).set(
    {
      user: emailAddress,
      messageType: "mail",
      threadId: m.data.threadId,
      internalDate: internalDateMs || null,
      receivedAt: internalDateMs ? new Date(internalDateMs) : new Date(),
      from,
      to,
      cc,
      subject,
      snippet: m.data.snippet || "",
      textPlain,
      textHtml,
      labels: m.data.labelIds || [],
      attachments,
      gcsBucket: GCS_BUCKET || null,
      createdAt: Date.now(),
    },
    { merge: true }
  );

  console.log(
    `[MAIL] saved message ${m.data.id} (attachments: ${attachments.length})`
  );
}
