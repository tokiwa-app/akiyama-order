import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";
import path from "path";
import { supabase } from "./supabaseClient.js";

/* ================= ENV ================= */
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI = process.env.OAUTH_REDIRECT_URI || "";
const OAUTH_REFRESH_TOKEN = process.env.OAUTH_REFRESH_TOKEN || null;
const FAX_SENDER = (process.env.FAX_SENDER || "").toLowerCase();
const LOOKBACK_MINUTES = Number(process.env.LOOKBACK_MINUTES || 10);

/* ================= INIT ================= */
const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;

/* ================= UTIL ================= */
function b64UrlDecode(data) {
  return Buffer.from((data || "").replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}
function getHeader(headers, name) {
  const h = (headers || []).find((x) => x.name?.toLowerCase() === name.toLowerCase());
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
    if (p.mimeType === "text/plain" && p.body?.data) textPlain += b64UrlDecode(p.body.data);
    if (p.mimeType === "text/html" && p.body?.data) textHtml += b64UrlDecode(p.body.data);
  }
  return { textPlain, textHtml };
}
function safeFilename(name) {
  const base = path.posix.basename(name || "attachment");
  return encodeURIComponent(base.replace(/[\u0000-\u001F\u007F/\\]/g, "_"));
}

/* ================= Gmail ================= */
let cachedRefreshToken = OAUTH_REFRESH_TOKEN;
async function getGmail() {
  const oAuth2 = new google.auth.OAuth2(
    OAUTH_CLIENT_ID,
    OAUTH_CLIENT_SECRET,
    OAUTH_REDIRECT_URI
  );
  oAuth2.setCredentials({ refresh_token: cachedRefreshToken });
  return google.gmail({ version: "v1", auth: oAuth2 });
}

async function saveAttachmentToGCS(userEmail, messageId, part) {
  if (!bucket) return null;
  const gmail = await getGmail();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: part.body.attachmentId,
  });

  const bytes = Buffer.from(
    (res.data.data || "").replace(/-/g, "+").replace(/_/g, "/"),
    "base64"
  );

  const filename = safeFilename(part.filename);
  const objectPath = `gmail/${encodeURIComponent(userEmail)}/${messageId}/${filename}`;

  await bucket.file(objectPath).save(bytes, { resumable: false });
  return `gs://${GCS_BUCKET}/${objectPath}`;
}

/* ================= HANDLER ================= */
export default async function gmailPoll(req, res) {
  try {
    const gmail = await getGmail();
    const profile = await gmail.users.getProfile({ userId: "me" });
    const emailAddress = profile.data.emailAddress || "me";

    const minutes = Number(req.query?.minutes || LOOKBACK_MINUTES);
    const cutoffEpoch = Math.floor((Date.now() - minutes * 60000) / 1000);
    const q = `after:${cutoffEpoch} in:inbox`;

    const list = await gmail.users.messages.list({ userId: "me", q });
    const ids = (list.data.messages || []).map((m) => m.id);

    for (const id of ids) {
      const { data: exists } = await supabase
        .from("messages")
        .select("id")
        .eq("id", id)
        .maybeSingle();
      if (exists) continue;

      const full = await gmail.users.messages.get({ userId: "me", id, format: "full" });
      const payload = full.data.payload;
      const headers = payload.headers || [];

      const subject = getHeader(headers, "Subject");
      const from = getHeader(headers, "From");
      const dateHdr = getHeader(headers, "Date");

      const receivedAt = full.data.internalDate
        ? new Date(Number(full.data.internalDate))
        : new Date(dateHdr);

      const addrMatch = (from || "").match(/<([^>]+)>/);
      const fromEmail = (addrMatch ? addrMatch[1] : from).toLowerCase();
      const messageType = fromEmail === FAX_SENDER ? "fax" : "mail";

      const managementNo = `gmail_${full.data.threadId || id}`;
      const { data: caseRow } = await supabase
        .from("cases")
        .upsert({ management_no: managementNo })
        .select()
        .single();

      const { textPlain, textHtml } = extractBodies(payload);

      await supabase.from("messages").insert({
        id,
        case_id: caseRow.id,
        message_type: messageType,
        subject,
        from_email: from,
        received_at: receivedAt.toISOString(),
        body_text: textPlain || textHtml,
        ocr_status: "pending",
      });

      const parts = flattenParts(payload.parts || []);
      const attachments = [];
      for (const p of parts) {
        if (p.filename && p.body?.attachmentId) {
          const uri = await saveAttachmentToGCS(emailAddress, id, p);
          if (uri) attachments.push(uri);
        }
      }

      if (messageType === "fax" && attachments[0]) {
        await supabase.from("message_main_pdf_files").insert({
          case_id: caseRow.id,
          message_id: id,
          gcs_path: attachments[0],
          file_type: "fax_original",
        });
      }
    }

    res.status(200).send("ok");
  } catch (e) {
    console.error(e);
    res.status(500).send("error");
  }
}
