// faxHandler.js
import { getHeader, extractBodies, flattenParts, saveAttachmentToGCS } from "./utils.js";
import { db, GCS_BUCKET } from "./globals.js";

export async function handleFaxMail(m, payload, emailAddress) {
  const headers = payload?.headers || [];
  const subject = getHeader(headers, "Subject");
  const from = getHeader(headers, "From");
  const to = getHeader(headers, "To");
  const dateHdr = getHeader(headers, "Date");
  const internalDateMs = m.data.internalDate ? Number(m.data.internalDate) : Date.parse(dateHdr);

  const { textPlain, textHtml } = extractBodies(payload);

  const attachments = [];
  const parts = flattenParts(payload?.parts || []);
  for (const p of parts) {
    if (p?.filename && p.body?.attachmentId) {
      const path = await saveAttachmentToGCS(emailAddress, m.data.id, p);
      if (path) attachments.push(path);
    }
  }

  await db.collection("messages").doc(m.data.id).set(
    {
      user: emailAddress,
      messageType: "fax", // ← FAX区分
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
    },
    { merge: true }
  );

  console.log(`[FAX] saved message ${m.data.id} (attachments: ${attachments.length})`);
}


