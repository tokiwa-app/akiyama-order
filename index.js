// index.js
import express from "express";
import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";
import path from "path";
import { supabase } from "./supabaseClient.js";

import { registerIngestRoutes } from "./ingest.js";

// ================== ENV ==================
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI = process.env.OAUTH_REDIRECT_URI || "";
const OAUTH_REFRESH_TOKEN = process.env.OAUTH_REFRESH_TOKEN || null;

const FAX_SENDERS = (process.env.FAX_SENDERS || "")
  .toLowerCase()
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

const LOOKBACK_MINUTES = Number(process.env.LOOKBACK_MINUTES || 10);

// ================== INIT ==================
const app = express();
app.use(express.json());

const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;

// ================== UTIL ==================
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

function safeFilename(name) {
  const base = path.posix.basename(name || "attachment");
  return encodeURIComponent(
    base.replace(/[\u0000-\u001F\u007F/\\]/g, "_")
  );
}

// ================== Gmail Auth ==================
let cachedRefreshToken = OAUTH_REFRESH_TOKEN;

async function getGmail() {
  if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET || !cachedRefreshToken) {
    throw new Error("OAuth not configured (client id/secret/refresh token).");
  }

  const oAuth2 = new google.auth.OAuth2(
    OAUTH_CLIENT_ID,
    OAUTH_CLIENT_SECRET,
    OAUTH_REDIRECT_URI
  );

  oAuth2.setCredentials({ refresh_token: cachedRefreshToken });

  return google.gmail({ version: "v1", auth: oAuth2 });
}

// ================== GCS Attach Save ==================
async function saveAttachmentToGCS(userEmail, gmailMessageId, part) {
  if (!bucket) return null;

  const attachId = part?.body?.attachmentId;
  if (!attachId) return null;

  const gmail = await getGmail();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId: gmailMessageId,
    id: attachId,
  });

  const b64 = (res.data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  const bytes = Buffer.from(b64, "base64");
  const filename = safeFilename(part.filename || `attachment-${attachId}`);
  const objectPath = `gmail/${encodeURIComponent(
    userEmail
  )}/${gmailMessageId}/${filename}`;

  await bucket.file(objectPath).save(bytes, {
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" },
  });

  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ================== Supabase helpers ==================
async function upsertCaseByManagementNo(
  managementNo,
  customerId,
  customerName,
  title,
  receivedAtIso
) {
  const { data: existing, error: selErr } = await supabase
    .from("cases")
    .select("id")
    .eq("management_no", managementNo)
    .maybeSingle();

  if (selErr) throw selErr;

  if (existing) {
    const { error: updErr } = await supabase
      .from("cases")
      .update({
        customer_id: customerId,
        customer_name: customerName,
        latest_message_at: receivedAtIso,
        title: title ?? null,
      })
      .eq("id", existing.id);

    if (updErr) throw updErr;
    return existing.id;
  }

  const { data: inserted, error: insErr } = await supabase
    .from("cases")
    .insert({
      management_no: managementNo,
      customer_id: customerId,
      customer_name: customerName,
      title: title ?? null,
      latest_message_at: receivedAtIso,
    })
    .select()
    .single();

  if (insErr) throw insErr;

  // 登録直後に main_case_id = 自分(id)
  const { error: mainErr } = await supabase
    .from("cases")
    .update({ main_case_id: inserted.id })
    .eq("id", inserted.id);

  if (mainErr) throw mainErr;

  // フラグ code=0 を付ける
  const { error: flagErr } = await supabase
    .from("case_flag_links")
    .upsert(
      { case_id: inserted.id, flag_code: "0" },
      { onConflict: "case_id,flag_code" }
    );

  if (flagErr) throw flagErr;

  return inserted.id;
}

// ================== ROUTES ==================
app.get("/", (_req, res) => res.status(200).send("ok"));

const deps = {
  FAX_SENDERS,
  LOOKBACK_MINUTES,
  supabase,
  getGmail,
  getHeader,
  flattenParts,
  saveAttachmentToGCS,
  upsertCaseByManagementNo,
};

registerIngestRoutes(app, deps);

// ================== START ==================
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));
