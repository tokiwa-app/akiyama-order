// index.js
import express from "express";
import { google } from "googleapis";
import { Firestore } from "@google-cloud/firestore";
import { Storage } from "@google-cloud/storage";
import { CloudTasksClient } from "@google-cloud/tasks";
import pLimit from "p-limit";

import { handleFaxMail } from "./faxHandler.js";
import { handleNormalMail } from "./mailHandler.js";

// ==== 環境変数 ====
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID || "";
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET || "";
const OAUTH_REDIRECT_URI =
  process.env.OAUTH_REDIRECT_URI ||
  "https://<YOUR_CLOUD_RUN_URL>/oauth2/callback";
const GMAIL_TOPIC =
  process.env.GMAIL_TOPIC ||
  "projects/tokiwa-cloud-auth-25c0c/topics/gmail-inbox";

// Cloud Tasks 用
const CLOUD_RUN_BASE_URL = process.env.CLOUD_RUN_BASE_URL; // 例: https://akiyama-order-xxxx.a.run.app
const TASKS_LOCATION = process.env.TASKS_LOCATION || "asia-northeast2";
const TASKS_QUEUE_ID = "gmail-history";

// ==== 初期化 ====
const app = express();
app.use(express.json());

export const db = new Firestore(
  FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {}
);
export const storage = new Storage();
export const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;

// ★ CloudTasksClient の変数名を変更（tasks と衝突防止）
const cloudTasks = new CloudTasksClient();

// ==== OAuth リフレッシュトークン保存 ====
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
  await db
    .collection("system")
    .doc("gmail_oauth")
    .set({ refresh_token: token, updatedAt: Date.now() }, { merge: true });
}

// ==== Gmail クライアント ====
async function getGmail() {
  if (OAUTH_CLIENT_ID && OAUTH_CLIENT_SECRET) {
    const refresh = await getStoredRefreshToken();
    if (refresh) {
      const oAuth2 = new google.auth.OAuth2(
        OAUTH_CLIENT_ID,
        OAUTH_CLIENT_SECRET,
        OAUTH_REDIRECT_URI
      );
      oAuth2.setCredentials({ refresh_token: refresh });
      return google.gmail({ version: "v1", auth: oAuth2 });
    }
  }
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
  });
  const client = await auth.getClient();
  return google.gmail({ version: "v1", auth: client });
}

// ==== Gmail History 差分処理 ====
async function handleHistory(emailAddress, historyId) {
  const gmail = await getGmail();
  const userDoc = db.collection("gmail_users").doc(emailAddress);
  const snap = await userDoc.get();
  const lastHistoryId = snap.exists ? snap.data().lastHistoryId : null;

  if (!lastHistoryId) {
    await userDoc.set(
      { lastHistoryId: historyId, updatedAt: Date.now() },
      { merge: true }
    );
    console.log(`[INIT] historyId=${historyId} saved for ${emailAddress}`);
    return;
  }

  const newIds = new Set();
  let pageToken;
  try {
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
  } catch (err) {
    if (err?.code === 404) {
      console.warn("History too old. Doing backfill...");
      let listPageToken;
      do {
        const lr = await gmail.users.messages.list({
          userId: "me",
          q: "newer_than:30d in:inbox",
          pageToken: listPageToken,
          maxResults: 200,
        });
        listPageToken = lr.data.nextPageToken || null;
        for (const m of lr.data.messages || []) {
          if (m.id) newIds.add(m.id);
        }
      } while (listPageToken);
    } else {
      throw err;
    }
  }

  console.log(
    `Found ${newIds.size} new messages since ${lastHistoryId} for ${emailAddress}`
  );

  const limit = pLimit(4); // 同時4件まで処理
  const workerPromises = [];

  for (const id of newIds) {
    workerPromises.push(
      limit(async () => {
        const m = await gmail.users.messages.get({
          userId: "me",
          id,
          format: "full",
        });
        const payload = m.data.payload;
        const headers = payload?.headers || [];
        const from =
          headers.find((x) => x.name?.toLowerCase() === "from")?.value || "";

        if (from.includes("akiyama.order@gmail.com")) {
          await handleFaxMail(m, payload, emailAddress, db, GCS_BUCKET, gmail);
        } else {
          await handleNormalMail(
            m,
            payload,
            emailAddress,
            db,
            GCS_BUCKET,
            gmail
          );
        }
      })
    );
  }

  await Promise.allSettled(workerPromises);

  // lastHistoryId の逆行更新を防ぐ
  const current = await userDoc.get();
  const curId = current.exists ? current.data().lastHistoryId : null;
  if (curId && BigInt(curId) > BigInt(historyId)) {
    console.log(
      `[SKIP] newer historyId already stored (${curId} > ${historyId})`
    );
    return;
  }

  await userDoc.set(
    { lastHistoryId: historyId, updatedAt: Date.now() },
    { merge: true }
  );
}

// ==== Cloud Tasks enqueue ====
async function enqueueHistoryJob(payload) {
  const project = await cloudTasks.getProjectId();
  const parent = cloudTasks.queuePath(project, TASKS_LOCATION, TASKS_QUEUE_ID);
  if (!CLOUD_RUN_BASE_URL)
    throw new Error("CLOUD_RUN_BASE_URL is not configured");

  const url = `${CLOUD_RUN_BASE_URL}/tasks/gmail-history`;
  const serviceAccountEmail = `tasks-invoker@${project}.iam.gserviceaccount.com`;

  // ★ base64 せず、生 JSON を送信
  await cloudTasks.createTask({
    parent,
    task: {
      httpRequest: {
        httpMethod: "POST",
        url,
        headers: { "Content-Type": "application/json" },
        body: Buffer.from(JSON.stringify(payload)),
        oidcToken: { serviceAccountEmail },
      },
    },
  });
}

// ==== ルーティング ====

// ヘルスチェック
app.get("/", (_req, res) => res.status(200).send("ok"));

// Gmail Push（Cloud Tasks に委譲）
app.post("/gmail/push", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.data) return res.status(204).send();
    const decoded = Buffer.from(msg.data, "base64").toString("utf8");
    const { emailAddress, historyId } = JSON.parse(decoded);
    if (!emailAddress || !historyId) return res.status(204).send();

    res.status(204).send();

    enqueueHistoryJob({ emailAddress, historyId }).catch((e) =>
      console.error("[enqueueHistoryJob] failed:", e)
    );
  } catch (e) {
    console.error("Push handler error:", e);
    return res.status(204).send();
  }
});

// Cloud Tasks worker（実際の処理）
app.post("/tasks/gmail-history", async (req, res) => {
  try {
    let body = req.body;
    // ★ 保険：古いタスクで文字列になっていてもパース
    if (typeof body === "string") {
      try {
        body = JSON.parse(body);
      } catch {}
    }
    const { emailAddress, historyId } = body || {};
    if (!emailAddress || !historyId) return res.status(400).send("bad request");

    console.log(`🛠️ tasks/gmail-history start: ${emailAddress}`);
    await handleHistory(emailAddress, historyId);
    console.log(`✅ tasks/gmail-history done: ${emailAddress}`);
    return res.status(204).send();
  } catch (e) {
    console.error("tasks/gmail-history error:", e);
    return res.status(500).send();
  }
});

// OAuth 関連
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
    console.error("oauth2/start error", e);
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
    oAuth2.setCredentials(tokens);

    if (tokens.refresh_token) await storeRefreshToken(tokens.refresh_token);

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
    await db.collection("gmail_users").doc(profile.data.emailAddress).set(
      {
        lastHistoryId: watchRes.data.historyId || null,
        watchExpiration: watchRes.data.expiration || null,
        updatedAt: Date.now(),
      },
      { merge: true }
    );

    res
      .status(200)
      .send(`OK\nwatch historyId=${watchRes.data.historyId || "(none)"}\n`);
  } catch (e) {
    console.error("oauth2/callback error", e);
    res.status(500).send("oauth callback error");
  }
});

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
    await db.collection("gmail_users").doc(profile.data.emailAddress).set(
      {
        lastHistoryId: watchRes.data.historyId || null,
        watchExpiration: watchRes.data.expiration || null,
        updatedAt: Date.now(),
      },
      { merge: true }
    );
    res.status(200).send("renewed");
  } catch (e) {
    console.error("watch renew error", e);
    res.status(500).send("renew error");
  }
});

// ==== 例外ログ ====
process.on("unhandledRejection", (e) =>
  console.error("[FATAL] unhandledRejection:", e)
);
process.on("uncaughtException", (e) =>
  console.error("[FATAL] uncaughtException:", e)
);

// ==== 起動 ====
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => {
  console.log(`listening on ${port}`);
});
