// index.js (Supabase master / Firestore only for customer lookup)
import express from "express";
import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";
import path from "path";
import vision from "@google-cloud/vision";
import { Firestore } from "@google-cloud/firestore";
import { supabase } from "./supabaseClient.js";

import { registerIngestRoutes } from "./ingest.js";
import { registerProcessBatchRoutes } from "./processBatch.js";

// ================== ENV ==================
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI = process.env.OAUTH_REDIRECT_URI || "";
const OAUTH_REFRESH_TOKEN = process.env.OAUTH_REFRESH_TOKEN || null;

const FAX_SENDER = (process.env.FAX_SENDER || "").toLowerCase();
const LOOKBACK_MINUTES = Number(process.env.LOOKBACK_MINUTES || 10);

// Firestore (customer master only)
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;
const customerDb = new Firestore(
  FIREBASE_PROJECT_ID
    ? { projectId: FIREBASE_PROJECT_ID, databaseId: "akiyama-system" }
    : { databaseId: "akiyama-system" }
);

// ================== INIT ==================
const app = express();
app.use(express.json());

const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;
const visionClient = new vision.ImageAnnotatorClient();

// ================== UTIL ==================
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
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`TIMEOUT ${label} after ${ms}ms`)), ms)),
  ]);
}
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

// ================== Gmail Auth ==================
let cachedRefreshToken = OAUTH_REFRESH_TOKEN;
async function getGmail() {
  if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET || !cachedRefreshToken) {
    throw new Error("OAuth not configured (client id/secret/refresh token).");
  }
  const oAuth2 = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REDIRECT_URI);
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
  const objectPath = `gmail/${encodeURIComponent(userEmail)}/${gmailMessageId}/${filename}`;

  await bucket.file(objectPath).save(bytes, {
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" },
  });

  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ================== Firestore Customer Lookup (only) ==================
async function detectCustomerFromMaster(sourceText) {
  try {
    const snap = await customerDb.collection("jsons").doc("Client Search").get();
    if (!snap.exists) return null;

    const doc = snap.data();
    let sheet = doc.main;
    if (typeof sheet === "string") sheet = JSON.parse(sheet);

    const matrix = sheet?.tables?.[0]?.matrix;
    if (!Array.isArray(matrix) || matrix.length < 2) return null;

    const header = matrix[0];
    const idx = (colName) => header.indexOf(colName);

    const colId = idx("id");
    const colName = idx("name");
    const colMailAliases = idx("mail_aliases");
    const colFaxAliases = idx("fax_aliases");
    const colNameAliases = idx("name_aliases");

    if (colId === -1 || colName === -1) return null;

    const normalize = (str) => String(str || "").toLowerCase().replace(/\s+/g, "");
    const normalizeDigits = (str) => String(str || "").replace(/[^\d]/g, "");

    const textNorm = normalize(sourceText);
    const textDigits = normalizeDigits(sourceText);

    const split = (v) =>
      String(v || "")
        .split(/[,\s、;／]+/)
        .map((x) => x.trim())
        .filter(Boolean);

    const rows = matrix.slice(1).map((row) => ({
      id: row[colId],
      name: row[colName],
      mailAliases: colMailAliases !== -1 ? split(row[colMailAliases]) : [],
      faxAliases: colFaxAliases !== -1 ? split(row[colFaxAliases]) : [],
      nameAliases: colNameAliases !== -1 ? split(row[colNameAliases]) : [],
    }));

    for (const r of rows) {
      for (const a of r.mailAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) return { id: r.id, name: r.name };
      }
    }
    for (const r of rows) {
      for (const a of r.faxAliases) {
        const aDigits = normalizeDigits(a);
        if (aDigits && textDigits.includes(aDigits)) return { id: r.id, name: r.name };
      }
    }
    for (const r of rows) {
      for (const a of r.nameAliases) {
        const aNorm = normalize(a);
        if (aNorm && textNorm.includes(aNorm)) return { id: r.id, name: r.name };
      }
    }
  } catch (e) {
    console.error("detectCustomerFromMaster error:", e);
  }
  return null;
}

// ================== OCR (FAX) ==================
async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "" };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random().toString(36).slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const [op] = await visionClient.asyncBatchAnnotateFiles({
    requests: [
      {
        inputConfig: { gcsSource: { uri: gcsUri }, mimeType },
        features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
        outputConfig: { gcsDestination: { uri: outUri }, batchSize: 1 },
        pages: [1],
      },
    ],
  });

  await withTimeout(op.promise(), 150000, "vision op.promise");

  const [files] = await storage.bucket(info.bucket).getFiles({ prefix: tmpPrefix });
  if (!files.length) return { text: "" };

  const [buf] = await files[0].download();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const text = json.responses?.[0]?.fullTextAnnotation?.text || "";
  return { text };
}

async function runOcrForAttachments(attachments) {
  let full = "";
  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

    try {
      let r;
      if (lower.endsWith(".pdf")) r = await ocrFirstPageFromFile(uri, "application/pdf");
      else if (lower.endsWith(".tif") || lower.endsWith(".tiff")) r = await ocrFirstPageFromFile(uri, "image/tiff");
      else {
        const [res] = await withTimeout(
          visionClient.documentTextDetection({ image: { source: { imageUri: uri } } }),
          60000,
          "vision documentTextDetection(image)"
        );
        const text =
          res.fullTextAnnotation?.text ||
          res.textAnnotations?.[0]?.description ||
          "";
        r = { text };
      }

      if (r?.text) full += (full ? "\n" : "") + r.text;
    } catch (e) {
      console.warn("OCR failed:", uri, e?.message || e);
    }
  }
  return full;
}

// ================== Supabase helpers ==================
async function upsertCaseByManagementNo(managementNo, customerId, customerName, title, receivedAtIso) {
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
      assignee_id: 4,
    })
    .select()
    .single();

  if (insErr) throw insErr;

// ★追加：登録直後に main_case_id = 自分(id)
const { error: mainErr } = await supabase
  .from("cases")
  .update({ main_case_id: inserted.id })
  .eq("id", inserted.id);

if (mainErr) throw mainErr;

// untriaged を付ける（case_flag_links）
const { error: flagErr } = await supabase
  .from("case_flag_links")
  .upsert(
    { case_id: inserted.id, flag_code: "untriaged" },
    { onConflict: "case_id,flag_code" }
  );

if (flagErr) throw flagErr;

// ★ confirm を付ける（進捗タグ）
const { error: tagErr } = await supabase
  .from("case_tag_links")
  .upsert(
    { case_id: inserted.id, tag_code: "confirm" },
    { onConflict: "case_id,tag_code" }
  );

if (tagErr) throw tagErr;

return inserted.id;

}

// ================== ROUTES ==================
app.get("/", (_req, res) => res.status(200).send("ok"));

// ルート登録（ロジックは各ファイルへ移動）
const deps = {
  // env-ish
  FAX_SENDER,
  LOOKBACK_MINUTES,

  // clients
  supabase,
  storage,
  bucket,
  visionClient,
  customerDb,

  // shared funcs
  getGmail,
  getHeader,
  flattenParts,
  extractBodies,
  saveAttachmentToGCS,
  runOcrForAttachments,
  detectCustomerFromMaster,
  upsertCaseByManagementNo,
};

registerIngestRoutes(app, deps);
registerProcessBatchRoutes(app, deps);

// ================== START ==================
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));
