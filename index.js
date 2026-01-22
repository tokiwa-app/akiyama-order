// index.js (Supabase master / Firestore only for customer lookup)
import express from "express";
import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";
import path from "path";
import vision from "@google-cloud/vision";
import { Firestore } from "@google-cloud/firestore";
import { supabase } from "./supabaseClient.js";

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
    })
    .select()
    .single();

  if (insErr) throw insErr;
  return inserted.id;
}

// ================== ROUTES ==================
app.get("/", (_req, res) => res.status(200).send("ok"));

/**
 * 1) Gmail → Supabase ingest
 * - 先に cases を作る（messages.case_id NOT NULL 前提）
 * - 添付は GCS 保存
 * - mail: message_attachments に保存（case_id必須）
 * - fax : message_main_pdf_files だけ保存（case_id必須）
 * - OCR/顧客特定はここでやらない
 */
app.post("/gmail/poll", async (req, res) => {
  const started = Date.now();
  try {
    const gmail = await getGmail();
    const profile = await gmail.users.getProfile({ userId: "me" });
    const emailAddress = profile.data.emailAddress || "me";

    const minutesParam = Number(req.query?.minutes || LOOKBACK_MINUTES);
    const minutes = Number.isFinite(minutesParam) && minutesParam > 0 ? Math.floor(minutesParam) : LOOKBACK_MINUTES;

    const cutoffEpoch = Math.floor((Date.now() - minutes * 60 * 1000) / 1000);
    const q = `after:${cutoffEpoch} in:inbox`;

    let pageToken;
    let seen = 0;
    let ingested = 0;

    do {
      const list = await gmail.users.messages.list({
        userId: "me",
        q,
        pageToken,
        maxResults: 200,
      });

      pageToken = list.data.nextPageToken || null;
      const ids = (list.data.messages || []).map((m) => m.id);
      if (!ids.length) break;

      for (const id of ids) {
        seen++;

        const { data: existsRow, error: exErr } = await supabase
          .from("messages")
          .select("id")
          .eq("id", id)
          .maybeSingle();
        if (exErr) throw exErr;
        if (existsRow) continue;

        const full = await gmail.users.messages.get({ userId: "me", id, format: "full" });

        const payload = full.data.payload;
        const headers = payload?.headers || [];
        const subject = getHeader(headers, "Subject");
        const from = getHeader(headers, "From");
        const to = getHeader(headers, "To");
        const cc = getHeader(headers, "Cc");
        const dateHdr = getHeader(headers, "Date");

        const internalDateMs = full.data.internalDate ? Number(full.data.internalDate) : Date.parse(dateHdr);
        const receivedAt = internalDateMs ? new Date(internalDateMs) : new Date();
        const receivedAtIso = receivedAt.toISOString();

        const { textPlain, textHtml } = extractBodies(payload);

        const addrMatch = (from || "").match(/<([^>]+)>/);
        const fromEmail = (addrMatch ? addrMatch[1] : (from || "")).trim().toLowerCase();
        const messageType = FAX_SENDER && fromEmail === FAX_SENDER ? "fax" : "mail";

        const threadId = full.data.threadId || id;
        const managementNo = `gmail_${threadId}`;

        const caseId = await upsertCaseByManagementNo(
          managementNo,
          null,
          null,
          subject ?? null,
          receivedAtIso
        );

        const { error: insMsgErr } = await supabase
          .from("messages")
          .insert({
            id,
            case_id: caseId,
            message_type: messageType,
            subject: subject ?? null,
            from_email: from ?? null,
            to_email: to ?? null,
            received_at: receivedAtIso,
            snippet: full.data.snippet ?? null,
            main_pdf_path: null,
            body_text: textPlain ? textPlain : (textHtml ? textHtml : ""),
            body_type: messageType === "fax" ? "fax_raw" : "mail_raw",
            processed_at: null,
            processing_at: null,
            ocr_status: "pending",
          });

        if (insMsgErr) throw insMsgErr;

        // 添付保存（GCS）
        const attachments = [];
        const parts = flattenParts(payload?.parts || []);
        for (const p of parts) {
          if (p?.filename && p.body?.attachmentId) {
            const gsPath = await saveAttachmentToGCS(emailAddress, id, p);
            if (gsPath) attachments.push(gsPath);
          }
        }

        if (messageType === "mail") {
          if (attachments.length > 0) {
            const rows = attachments.map((p) => ({
              case_id: caseId,
              message_id: id,
              gcs_path: p,
              file_name: typeof p === "string" ? p.split("/").pop() || null : null,
              mime_type: null,
            }));
            const { error: attErr } = await supabase.from("message_attachments").insert(rows);
            if (attErr) console.error("insert message_attachments error:", attErr);
          }
        } else {
          const pdf = attachments.find((p) => typeof p === "string" && p.toLowerCase().endsWith(".pdf"));
          const any = attachments.find((p) => typeof p === "string");
          const mainPdfPath = pdf || any || null;

          if (mainPdfPath) {
            await supabase.from("messages").update({ main_pdf_path: mainPdfPath }).eq("id", id);

            const row = {
              case_id: caseId,
              message_id: id,
              gcs_path: mainPdfPath,
              file_name: typeof mainPdfPath === "string" ? mainPdfPath.split("/").pop() || null : null,
              mime_type: "application/pdf",
              file_type: "fax_original",
              thumbnail_path: null,
            };
            const { error: mainErr } = await supabase.from("message_main_pdf_files").insert(row);
            if (mainErr) console.error("insert message_main_pdf_files error:", mainErr);
          }
        }

        ingested++;
      }
    } while (pageToken);

    res.status(200).send(`OK seen=${seen} ingested=${ingested} minutes=${minutes} in ${Date.now() - started}ms`);
  } catch (e) {
    console.error("/gmail/poll error:", e);
    res.status(500).send("error");
  }
});

/**
 * 2) pending を少量処理
 * - messages から未処理を取ってロック
 * - fax: message_main_pdf_files からPDF取得 → OCR → 顧客特定 → cases/messages 更新
 * - mail: body_text で顧客特定（必要なら）
 */
app.post("/gmail/process-batch", async (req, res) => {
  const started = Date.now();
  const limit = Math.min(Number(req.query?.limit || 2), 10);
  const lockMinutes = 10;

  try {
    const { data: rows, error: selErr } = await supabase
      .from("messages")
      .select("id, case_id, message_type, subject, body_text, received_at")
      .is("processed_at", null)
      .or(`processing_at.is.null,processing_at.lt.${new Date(Date.now() - lockMinutes * 60 * 1000).toISOString()}`)
      .order("received_at", { ascending: false })
      .limit(limit);

    if (selErr) throw selErr;

    let processed = 0;

    for (const m of rows || []) {
      const nowIso = new Date().toISOString();
      const { error: lockErr } = await supabase
        .from("messages")
        .update({ processing_at: nowIso, ocr_status: "processing", ocr_error: null })
        .eq("id", m.id)
        .is("processed_at", null);

      if (lockErr) {
        console.error("lockErr:", lockErr);
        continue;
      }

      try {
        let attachments = [];
        let mainPdfPath = null;

        if (m.message_type === "fax") {
          const { data: pdfRows, error: pdfErr } = await supabase
            .from("message_main_pdf_files")
            .select("gcs_path")
            .eq("message_id", m.id)
            .order("created_at", { ascending: true })
            .limit(10);

          if (pdfErr) throw pdfErr;
          attachments = (pdfRows || []).map((r) => r.gcs_path).filter(Boolean);
          mainPdfPath = attachments[0] || null;
        } else {
          const { data: attRows } = await supabase
            .from("message_attachments")
            .select("gcs_path")
            .eq("message_id", m.id);
          attachments = (attRows || []).map((r) => r.gcs_path).filter(Boolean);
        }

        let ocrText = "";
        let customer = null;

        if (m.message_type === "fax") {
          ocrText = await runOcrForAttachments(attachments);
          const head = String(ocrText || "").slice(0, 200);
          customer = (head && (await detectCustomerFromMaster(head))) || (await detectCustomerFromMaster(ocrText));
        } else {
          const head = String(m.body_text || "").slice(0, 300);
          customer = (head && (await detectCustomerFromMaster(head))) || (await detectCustomerFromMaster(m.body_text || ""));
        }

        // cases 更新（顧客が取れた場合）
        if (m.case_id && (customer?.id || customer?.name)) {
          await supabase
            .from("cases")
            .update({
              customer_id: customer?.id ?? null,
              customer_name: customer?.name ?? null,
              latest_message_at: m.received_at ?? null,
              title: m.subject ?? null,
            })
            .eq("id", m.case_id);
        }

        const { error: updErr } = await supabase
          .from("messages")
          .update({
            main_pdf_path: mainPdfPath ?? null,
            ocr_text: ocrText,
            customer_id: customer?.id ?? null,
            customer_name: customer?.name ?? null,
            ocr_status: "done",
            processed_at: new Date().toISOString(),
            processing_at: null,
          })
          .eq("id", m.id);

        if (updErr) throw updErr;

        processed++;
      } catch (e) {
        console.error("process one error:", m.id, e);

        await supabase
          .from("messages")
          .update({
            ocr_status: "error",
            ocr_error: e?.message || String(e),
            processing_at: null,
          })
          .eq("id", m.id);
      }
    }

    res.status(200).send(`OK processed=${processed} picked=${(rows || []).length} in ${Date.now() - started}ms`);
  } catch (e) {
    console.error("/gmail/process-batch error:", e);
    res.status(500).send("error");
  }
});

// ================== START ==================
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));
