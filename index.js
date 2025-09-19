// index.js
// Cloud Run: Gmail Push を受けて Firestore / Cloud Storage に保存
// さらに Web OAuth で /oauth2/start → /oauth2/callback から watch を開始できます。

import express from "express";
import { google } from "googleapis";
import { Firestore } from "@google-cloud/firestore";
import { Storage } from "@google-cloud/storage";

// ==== 環境変数 ====
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined; // 例: tokiwa-cloud-auth-25c0c
const GCS_BUCKET = process.env.GCS_BUCKET || "";                          // 例: tokiwa-cloud-auth-25c0c.appspot.com

// OAuth（Web）
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI =
  process.env.OAUTH_REDIRECT_URI ||
  "https://akiyama-order-153993420990.asia-northeast2.run.app/oauth2/callback";

// Pub/Sub topic（Gmail Push 宛先）
const GMAIL_TOPIC = "projects/tokiwa-cloud-auth-25c0c/topics/gmail-inbox";

// ==== 初期化 ====
const app = express();
app.use(express.json());

const db = new Firestore(FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {});
const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;

// ==== ユーティリティ ====
function b64UrlDecode(data) {
  return Buffer.from((data || "").replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}
function getHeader(headers, name) {
  const h = (headers || []).find(x => x.name?.toLowerCase() === name.toLowerCase());
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

// ==== OAuth リフレッシュトークンの取得/保存（Firestore にも保存）====
let cachedRefreshToken = process.env.OAUTH_REFRESH_TOKEN || null;

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
  await db.collection("system").doc("gmail_oauth").set(
    { refresh_token: token, updatedAt: Date.now() },
    { merge: true }
  );
}

// ==== Gmail クライアント ====
// 1) OAUTH_CLIENT_ID/SECRET が設定されており、Refresh Token が Firestore or env にあれば OAuth を使用
// 2) なければ ADC（Cloud Run 実行 SA）で試行（Workspace + DWD 向け）
async function getGmail() {
  if (OAUTH_CLIENT_ID && OAUTH_CLIENT_SECRET) {
    const refresh = await getStoredRefreshToken();
    if (refresh) {
      const oAuth2 = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REDIRECT_URI);
      oAuth2.setCredentials({ refresh_token: refresh });
      return google.gmail({ version: "v1", auth: oAuth2 });
    }
  }
  const auth = new google.auth.GoogleAuth({ scopes: ["https://www.googleapis.com/auth/gmail.readonly"] });
  const client = await auth.getClient();
  return google.gmail({ version: "v1", auth: client });
}

// ==== 添付保存 ====
async function saveAttachmentToGCS(userEmail, messageId, part) {
  if (!bucket) return null;
  const attachId = part?.body?.attachmentId;
  if (!attachId) return null;
  const gmail = await getGmail();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: attachId,
  });
  const bytes = Buffer.from(res.data.data, "base64");
  const filename = part.filename || `attachment-${attachId}`;
  const objectPath = `gmail/${encodeURIComponent(userEmail)}/${messageId}/${filename}`;
  await bucket.file(objectPath).save(bytes, {
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" },
  });
  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ==== Gmail History 差分処理 ====
async function handleHistory(emailAddress, historyId) {
  const gmail = await getGmail();

  const userDoc = db.collection("gmail_users").doc(emailAddress);
  const snap = await userDoc.get();
  const lastHistoryId = snap.exists ? snap.data().lastHistoryId : null;

  // 初回は起点だけ保存
  if (!lastHistoryId) {
    await userDoc.set({ lastHistoryId: historyId, updatedAt: Date.now() }, { merge: true });
    console.log(`[INIT] historyId=${historyId} saved for ${emailAddress}`);
    return;
  }

  // 差分列挙
  const newIds = new Set();
  let pageToken;
  do {
    const r = await gmail.users.history.list({
      userId: "me",
      startHistoryId: lastHistoryId,
      historyTypes: ["messageAdded"],
      pageToken,
    });
    pageToken = r.data.nextPageToken || null;
    for (const h of r.data.history || []) {
      for (const ma of h.messagesAdded || []) {
        if (ma.message?.id) newIds.add(ma.message.id);
      }
    }
  } while (pageToken);

  console.log(`Found ${newIds.size} new messages since ${lastHistoryId} for ${emailAddress}`);

  // 取得→保存
  for (const id of newIds) {
    const m = await gmail.users.messages.get({
      userId: "me",
      id,
      format: "full",
    });

    const payload = m.data.payload;
    const headers = payload?.headers || [];
    const subject = getHeader(headers, "Subject");
    const from = getHeader(headers, "From");
    const to = getHeader(headers, "To");
    const cc = getHeader(headers, "Cc");
    const dateHdr = getHeader(headers, "Date");
    const internalDateMs = m.data.internalDate ? Number(m.data.internalDate) : Date.parse(dateHdr);

    const { textPlain, textHtml } = extractBodies(payload);

    // 添付
    const attachments = [];
    const parts = flattenParts(payload?.parts || []);
    for (const p of parts) {
      if (p?.filename && p.body?.attachmentId) {
        const path = await saveAttachmentToGCS(emailAddress, id, p);
        if (path) attachments.push(path);
      }
    }

    // Firestore（冪等）
    await db.collection("messages").doc(id).set(
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
        createdAt: Date.now(),
      },
      { merge: true }
    );

    console.log(`Saved message ${id} (attachments: ${attachments.length})`);
  }

  // 起点更新
  await userDoc.set({ lastHistoryId: historyId, updatedAt: Date.now() }, { merge: true });
}

// ==== ルーティング ====

// ヘルスチェック
app.get("/", (_req, res) => res.status(200).send("ok"));

// Gmail Push 受信
app.post("/gmail/push", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.data) return res.status(204).send();
    const decoded = Buffer.from(msg.data, "base64").toString("utf8");
    console.log("📩 Gmail Push Notification:", decoded);

    const { emailAddress, historyId } = JSON.parse(decoded);
    if (!emailAddress || !historyId) return res.status(204).send();

    await handleHistory(emailAddress, historyId);
    return res.status(200).send();
  } catch (e) {
    console.error("Push handler error:", e);
    return res.status(500).send();
  }
});

// OAuth: 同意フロー開始
app.get("/oauth2/start", async (_req, res) => {
  try {
    if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET) return res.status(400).send("OAuth client not set");
    const oAuth2 = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REDIRECT_URI);
    const url = oAuth2.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: ["https://www.googleapis.com/auth/gmail.readonly"],
    });
    res.redirect(url);
  } catch (e) {
    console.error("oauth2/start error", e);
    res.status(500).send("oauth start error");
  }
});

// OAuth: 同意後コールバック（Refresh Token を Firestore に保存し、watch 開始）
app.get("/oauth2/callback", async (req, res) => {
  try {
    if (!OAUTH_CLIENT_ID || !OAUTH_CLIENT_SECRET) return res.status(400).send("OAuth client not set");
    const code = req.query?.code;
    if (!code) return res.status(400).send("missing code");

    const oAuth2 = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REDIRECT_URI);
    const { tokens } = await oAuth2.getToken(code);
    oAuth2.setCredentials(tokens);

    if (tokens.refresh_token) {
      await storeRefreshToken(tokens.refresh_token);
    }

    const gmail = google.gmail({ version: "v1", auth: oAuth2 });
    const watchRes = await gmail.users.watch({
      userId: "me",
      requestBody: {
        topicName: GMAIL_TOPIC,
        labelIds: ["INBOX"],
        labelFilterAction: "include",
      },
    });

    res.set("Content-Type", "text/plain");
    res.send(
      `OK\nstored_refresh_token=${tokens.refresh_token ? "yes" : "no"}\nwatch historyId=${watchRes.data.historyId || "(none)"}\n`
    );
  } catch (e) {
    console.error("oauth2/callback error", e);
    res.status(500).send("oauth callback error");
  }
});

// ==== 例外ログ ====
process.on("unhandledRejection", (e) => console.error("[FATAL] unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("[FATAL] uncaughtException:", e));

// ==== 起動 ====
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => {
  console.log(`listening on ${port}`);
});
