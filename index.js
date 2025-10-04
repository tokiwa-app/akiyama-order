// index.js
// Cloud Run: Gmail Push を受けて新しい Pub/Sub キューに転送する (高速処理専用)

import express from "express";
import { google } from "googleapis";
import { Firestore } from "@google-cloud/firestore";
import { Storage } from "@google-cloud/storage";
import { PubSub } from "@google-cloud/pubsub"; // 👈 追加

// ==== 環境変数 ====
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI =
  process.env.OAUTH_REDIRECT_URI ||
  "https://<YOUR_CLOUD_RUN_URL>/oauth2/callback";
  
const GMAIL_TOPIC = "projects/tokiwa-cloud-auth-25c0c/topics/gmail-inbox";
const PROCESS_QUEUE_TOPIC = "order-process-queue"; // 👈 新しい処理キューのトピック名

// ==== 初期化 ====
const app = express();
app.use(express.json());

const db = new Firestore(FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {});
const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;
const pubsub = new PubSub(FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {}); // 👈 Pub/Subクライアントの初期化

// ==== ユーティリティ (最小限のみ残す) ====
// ※ここでは getHeader, flattenParts, extractBodies など、メール解析用のユーティリティは不要となり、削除されています。

// ==== OAuth リフレッシュトークン保存 (変更なし) ====
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

// ==== Gmail クライアント (OAuth/Watch 用に変更なし) ====
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

// ==== ルーティング ====

// ヘルスチェック
app.get("/", (_req, res) => res.status(200).send("ok"));

// Gmail Push 受信 (フリーズ対策のため高速転送ロジックに変更)
app.post("/gmail/push", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.data) return res.status(204).send();
    const decoded = Buffer.from(msg.data, "base64").toString("utf8");
    
    // ログに出力
    console.log("📩 Gmail Push Notification (Received for transfer):", decoded);

    const { emailAddress, historyId } = JSON.parse(decoded);
    if (!emailAddress || !historyId) return res.status(204).send();

    // ⚡️ 重い処理はせず、通知データを新しい処理キューに転送する
    const dataToProcess = { emailAddress, historyId };
    const dataBuffer = Buffer.from(JSON.stringify(dataToProcess));
    
    // Pub/Subへのパブリッシュ実行
    await pubsub.topic(PROCESS_QUEUE_TOPIC).publishMessage({ data: dataBuffer });

    // 🌟 処理が終わるのを待たず、即座に 200 OK を返す (フリーズ回避)
    return res.status(200).send("Message queued for processing."); 
    
  } catch (e) {
    console.error("Push handler error (during transfer):", e);
    // Pub/Sub にエラーを返すとリトライされるリスクがあるため、500を返すか、200を返すかは状況次第ですが、ここではパブリッシュエラー時に500を返します。
    return res.status(500).send("Failed to queue message."); 
  }
});

// OAuth: 同意フロー開始 (変更なし)
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

// OAuth: コールバック (Watch 再登録ロジックに変更なし)
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

    const profile = await gmail.users.getProfile({ userId: "me" });
    await db.collection("gmail_users").doc(profile.data.emailAddress).set({
      lastHistoryId: watchRes.data.historyId || null,
      watchExpiration: watchRes.data.expiration || null,
      updatedAt: Date.now(),
    }, { merge: true });

    res.set("Content-Type", "text/plain");
    res.send(
      `OK\nstored_refresh_token=${tokens.refresh_token ? "yes" : "no"}\nwatch historyId=${watchRes.data.historyId || "(none)"}\n`
    );
  } catch (e) {
    console.error("oauth2/callback error", e);
    res.status(500).send("oauth callback error");
  }
});

// watch 更新 (変更なし)
app.post("/gmail/watch/renew", async (_req, res) => {
  try {
    const gmail = await getGmail();
    const profile = await gmail.users.getProfile({ userId: "me" });
    const watchRes = await gmail.users.watch({
      userId: "me",
      requestBody: {
        topicName: GMAIL_TOPIC,
        labelIds: ["INBOX"],
        labelFilterAction: "include",
      },
    });
    await db.collection("gmail_users").doc(profile.data.emailAddress).set({
      lastHistoryId: watchRes.data.historyId || null,
      watchExpiration: watchRes.data.expiration || null,
      updatedAt: Date.now(),
    }, { merge: true });
    res.status(200).send("renewed");
  } catch (e) {
    console.error("watch renew error", e);
    res.status(500).send("renew error");
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
