// index.js
import express from "express";
import { google } from "googleapis";
import { Firestore } from "@google-cloud/firestore";
import { Storage } from "@google-cloud/storage";
import path from "path";
import { runAfterProcess } from "./afterProcess.js"; // ← 追加

// ==== 環境変数 ====
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI =
  process.env.OAUTH_REDIRECT_URI ||
  "https://<YOUR_CLOUD_RUN_URL>/oauth2/callback";
const OAUTH_REFRESH_TOKEN = process.env.OAUTH_REFRESH_TOKEN || null;
const FAX_SENDER = (process.env.FAX_SENDER || "").toLowerCase();
const LOOKBACK_MINUTES = Number(process.env.LOOKBACK_MINUTES || 10);

// ==== 初期化 ====
const app = express();
app.use(express.json());
const db = new Firestore(
  FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {}
);
const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;

// ==== ユーティリティ ====
function b64UrlDecode(data) {
  return Buffer.from(
    (data || "").replace(/-/g, "+").replace(/_/g, "/"),
    "base64"
  ).toString("utf8");
}
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
    if (p.mimeType === "text/plain" && p.body?.data)
      textPlain += b64UrlDecode(p.body.data);
    if (p.mimeType === "text/html" && p.body?.data)
      textHtml += b64UrlDecode(p.body.data);
  }
  return { textPlain, textHtml };
}
function safeFilename(name) {
  const base = path.posix.basename(name || "attachment");
  return encodeURIComponent(base.replace(/[\u0000-\u001F\u007F/\\]/g, "_"));
}

// ==== OAuth 関連 ====
let cachedRefreshToken = OAUTH_REFRESH_TOKEN;
async function getStoredRefreshToken() {
  if (cachedRefreshToken) return cachedRefreshToken;
  const doc = await db.collection("system").doc("gmail_oauth").get();
  const tok = doc.exists ? doc.data()?.refresh_token : null;
  if (tok) cachedRefreshToken = tok;
  return cachedRefreshToken;
}
async function storeRefreshToken(token) {
  cachedRefreshToken = token || cachedRefreshToken;
  if (!token) return;
  await db
    .collection("system")
    .doc("gmail_oauth")
    .set({ refresh_token: token, updatedAt: Date.now() }, { merge: true });
}

async function getGmail() {
  const tStart = Date.now();
  const refresh = await getStoredRefreshToken();
  if (OAUTH_CLIENT_ID && OAUTH_CLIENT_SECRET && refresh) {
    const oAuth2 = new google.auth.OAuth2(
      OAUTH_CLIENT_ID,
      OAUTH_CLIENT_SECRET,
      OAUTH_REDIRECT_URI
    );
    oAuth2.setCredentials({ refresh_token: refresh });
    const gmail = google.gmail({ version: "v1", auth: oAuth2 });
    console.log("[gmail] getGmail done in", Date.now() - tStart, "ms");
    return gmail;
  }
  throw new Error("No OAuth refresh token available.");
}

// ==== 添付保存 ====
async function saveAttachmentToGCS(userEmail, messageId, part) {
  if (!bucket) return null;
  const attachId = part?.body?.attachmentId;
  if (!attachId) return null;

  const tGet = Date.now();
  const gmail = await getGmail();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: attachId,
  });
  console.log(
    "[attach] gmail.users.messages.attachments.get",
    { messageId, attachId },
    Date.now() - tGet,
    "ms"
  );

  const b64 = (res.data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  const bytes = Buffer.from(b64, "base64");
  const filename = safeFilename(part.filename || `attachment-${attachId}`);
  const objectPath = `gmail/${encodeURIComponent(
    userEmail
  )}/${messageId}/${filename}`;

  const tSave = Date.now();
  await bucket.file(objectPath).save(bytes, {
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" },
  });
  console.log(
    "[attach] GCS save",
    { objectPath },
    Date.now() - tSave,
    "ms"
  );

  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ==== メッセージ保存 ====
async function saveMessageDoc(emailAddress, m) {
  const tTotal = Date.now();
  console.log("[msg] saveMessageDoc start", m.data.id);

  const payload = m.data.payload;
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

  const addrMatch = (from || "").match(/<([^>]+)>/);
  const fromEmail = (addrMatch ? addrMatch[1] : (from || ""))
    .trim()
    .toLowerCase();
  const messageType =
    FAX_SENDER && fromEmail === FAX_SENDER ? "fax" : "mail";

  const attachments = [];
  const parts = flattenParts(payload?.parts || []);

  const tAttachLoop = Date.now();
  for (const p of parts) {
    if (p?.filename && p.body?.attachmentId) {
      const tOneAttach = Date.now();
      const path = await saveAttachmentToGCS(emailAddress, m.data.id, p);
      console.log(
        "[msg] one attachment processed",
        { messageId: m.data.id, filename: p.filename },
        Date.now() - tOneAttach,
        "ms"
      );
      if (path) attachments.push(path);
    }
  }
  console.log(
    "[msg] attachments loop done",
    { messageId: m.data.id, count: attachments.length },
    Date.now() - tAttachLoop,
    "ms"
  );

  const tSet = Date.now();
  await db
    .collection("messages")
    .doc(m.data.id)
    .set(
      {
        user: emailAddress,
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
        messageType,
        updatedAt: Date.now(),
        createdAt: Date.now(),
      },
      { merge: true }
    );
  console.log(
    "[msg] firestore messages.set",
    m.data.id,
    Date.now() - tSet,
    "ms"
  );

  const tAfter = Date.now();
  await runAfterProcess({ messageId: m.data.id, firestore: db, bucket });
  console.log(
    "[msg] runAfterProcess",
    m.data.id,
    Date.now() - tAfter,
    "ms"
  );

  console.log(
    "[msg] saveMessageDoc total",
    m.data.id,
    Date.now() - tTotal,
    "ms"
  );
}

// ==== ルーティング ====
app.get("/", (_req, res) => res.status(200).send("ok"));

app.get("/oauth2/start", async (_req, res) => {
  try {
    if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET)
      return res.status(400).send("OAuth client not set");
    const oAuth2 = new google.auth.OAuth2(
      OAUTH_CLIENT_ID,
      OAUTH_CLIENT_SECRET,
      OAUTH_REDIRECT_URI
    );
    const url = oAuth2.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: ["https://www.googleapis.com/auth/gmail.readonly"],
    });
    res.redirect(url);
  } catch (e) {
    console.error("/oauth2/start error:", e);
    res.status(500).send("oauth start error");
  }
});

app.get("/oauth2/callback", async (req, res) => {
  try {
    if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET)
      return res.status(400).send("OAuth client not set");
    const code = req.query?.code;
    if (!code) return res.status(400).send("missing code");
    const oAuth2 = new google.auth.OAuth2(
      OAUTH_CLIENT_ID,
      OAUTH_CLIENT_SECRET,
      OAUTH_REDIRECT_URI
    );
    const { tokens } = await oAuth2.getToken(code);
    if (tokens.refresh_token) await storeRefreshToken(tokens.refresh_token);
    res.status(200).send("linked");
  } catch (e) {
    console.error("/oauth2/callback error:", e);
    res.status(500).send("oauth callback error");
  }
});

app.post("/gmail/poll", async (req, res) => {
  const started = Date.now();
  console.log("[poll] start");

  try {
    const tGmail = Date.now();
    const gmail = await getGmail();
    console.log("[poll] getGmail", Date.now() - tGmail, "ms");

    const tProfile = Date.now();
    const profile = await gmail.users.getProfile({ userId: "me" });
    console.log("[poll] getProfile", Date.now() - tProfile, "ms");

    const emailAddress = profile.data.emailAddress || "me";
    const minutesParam = Number(req.query?.minutes || LOOKBACK_MINUTES);
    const minutes =
      Number.isFinite(minutesParam) && minutesParam > 0
        ? Math.floor(minutesParam)
        : LOOKBACK_MINUTES;

    const cutoffEpoch = Math.floor((Date.now() - minutes * 60 * 1000) / 1000);
    const q = `after:${cutoffEpoch} in:inbox`;

    let pageToken;
    let newCount = 0;
    let seen = 0;

    do {
      const tList = Date.now();
      const list = await gmail.users.messages.list({
        userId: "me",
        q,
        pageToken,
        maxResults: 200,
      });
      console.log("[poll] messages.list", Date.now() - tList, "ms");

      pageToken = list.data.nextPageToken || null;
      const ids = (list.data.messages || []).map((m) => m.id);
      if (!ids.length) break;

      for (const id of ids) {
        const tOne = Date.now();
        seen++;
        const doc = await db.collection("messages").doc(id).get();
        if (doc.exists) {
          console.log(
            "[poll] message already exists, skip",
            id,
            "in",
            Date.now() - tOne,
            "ms"
          );
          continue;
        }

        const tGetMsg = Date.now();
        const full = await gmail.users.messages.get({
          userId: "me",
          id,
          format: "full",
        });
        console.log(
          "[poll] messages.get",
          id,
          Date.now() - tGetMsg,
          "ms"
        );

        const tSave = Date.now();
        await saveMessageDoc(emailAddress, full);
        console.log(
          "[poll] saveMessageDoc",
          id,
          Date.now() - tSave,
          "ms"
        );

        console.log(
          "[poll] one message total",
          id,
          Date.now() - tOne,
          "ms"
        );
        newCount++;
      }
    } while (pageToken);

    const ms = Date.now() - started;
    console.log(
      "[poll] done",
      { seen, newCount, minutes },
      "in",
      ms,
      "ms"
    );
    res
      .status(200)
      .send(`OK processed=${seen} new=${newCount} minutes=${minutes} in ${ms}ms`);
  } catch (e) {
    console.error("/gmail/poll error:", e);
    res.status(200).send("OK (partial or error logged)");
  }
});

// ==== 起動 ====
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => {
  console.log(`listening on ${port}`);
});
