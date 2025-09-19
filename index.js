// index.js
// Gmail Push（Pub/Sub）を受けて、メール本文を取得し
// Firestore と Cloud Storage に保存する Cloud Run 用最小実装。

import express from "express";
import { google } from "googleapis";
import { Firestore } from "@google-cloud/firestore";
import { Storage } from "@google-cloud/storage";

// ====== 環境変数 ======
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID; // 例: tokiwa-cloud-auth-25c0c
const GCS_BUCKET = process.env.GCS_BUCKET;                   // 例: tokiwa-cloud-auth-25c0c.appspot.com

// （任意）OAuth 3LO を使う場合は下記3つを設定
// OAUTH_CLIENT_ID
// OAUTH_CLIENT_SECRET
// OAUTH_REFRESH_TOKEN

// ====== 初期化 ======
const app = express();
app.use(express.json());

const db = new Firestore(
  FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {}
);
const storage = new Storage();
const bucket = storage.bucket(GCS_BUCKET);

// ====== Gmail クライアント取得 ======
// 優先: OAuth 3LO（OAUTH_* が設定されているとき）
// 次点: Application Default Credentials（Cloud Run の実行 SA）
//
// - Google Workspace のドメインワイド委任（DWD）を使う場合は、
//   実行 SA に DWD を付与し、サービス アカウントの認可範囲に Gmail を追加してください。
//   （Cloud Run の ADC で動作）
async function getGmailClient() {
  const { OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REFRESH_TOKEN } = process.env;

  if (OAUTH_CLIENT_ID && OAUTH_CLIENT_SECRET && OAUTH_REFRESH_TOKEN) {
    const oAuth2Client = new google.auth.OAuth2(
      OAUTH_CLIENT_ID,
      OAUTH_CLIENT_SECRET
    );
    oAuth2Client.setCredentials({ refresh_token: OAUTH_REFRESH_TOKEN });
    return google.gmail({ version: "v1", auth: oAuth2Client });
  }

  // ADC（Cloud Run の実行 SA）を使用
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
  });
  const client = await auth.getClient();
  return google.gmail({ version: "v1", auth: client });
}

// ====== ユーティリティ ======
function b64UrlDecode(data) {
  // Gmail は URL-safe Base64
  return Buffer.from(
    (data || "").replace(/-/g, "+").replace(/_/g, "/"),
    "base64"
  ).toString("utf8");
}

function getHeader(headers, name) {
  const h = (headers || []).find(
    (x) => x.name && x.name.toLowerCase() === name.toLowerCase()
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
      textPlain += b64UrlDecode(p.body.data);
    } else if (p.mimeType === "text/html" && p.body?.data) {
      textHtml += b64UrlDecode(p.body.data);
    }
  }
  return { textPlain, textHtml };
}

async function saveAttachmentToGCS(userEmail, messageId, part) {
  const attachId = part?.body?.attachmentId;
  if (!attachId) return null;

  const gmail = await getGmailClient();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: attachId,
  });

  const bytes = Buffer.from(res.data.data, "base64");
  const filename = part.filename || `attachment-${attachId}`;
  const objectPath = `gmail/${encodeURIComponent(userEmail)}/${messageId}/${filename}`;

  const file = bucket.file(objectPath);
  await file.save(bytes, {
    resumable: false,
    metadata: {
      contentType: part.mimeType || "application/octet-stream",
    },
  });

  return `gs://${GCS_BUCKET}/${objectPath}`;
}

// ====== Gmail History 差分処理 ======
async function handleHistory(emailAddress, historyId) {
  const gmail = await getGmailClient();

  // ユーザー単位で前回 historyId を保持
  const userDoc = db.collection("gmail_users").doc(emailAddress);
  const userSnap = await userDoc.get();
  const lastHistoryId = userSnap.exists ? userSnap.data().lastHistoryId : null;

  // 初回は起点だけ保存して終了（大量フェッチ回避）
  if (!lastHistoryId) {
    await userDoc.set(
      { lastHistoryId: historyId, updatedAt: Date.now() },
      { merge: true }
    );
    console.log(`[INIT] set historyId=${historyId} for ${emailAddress}`);
    return;
  }

  // 差分列挙
  const newMessageIds = new Set();
  let pageToken;
  do {
    const res = await gmail.users.history.list({
      userId: "me",
      startHistoryId: lastHistoryId,
      historyTypes: ["messageAdded"],
      pageToken,
    });
    pageToken = res.data.nextPageToken || null;
    for (const h of res.data.history || []) {
      for (const ma of h.messagesAdded || []) {
        if (ma.message?.id) newMessageIds.add(ma.message.id);
      }
    }
  } while (pageToken);

  console.log(
    `Found ${newMessageIds.size} new messages since ${lastHistoryId} for ${emailAddress}`
  );

  // 各メッセージ取得 → Firestore/GCS へ保存
  for (const messageId of newMessageIds) {
    const m = await gmail.users.messages.get({
      userId: "me",
      id: messageId,
      format: "full",
    });

    const payload = m.data.payload;
    const headers = payload?.headers || [];
    const subject = getHeader(headers, "Subject");
    const from = getHeader(headers, "From");
    const to = getHeader(headers, "To");
    const cc = getHeader(headers, "Cc");
    const dateHeader = getHeader(headers, "Date");
    const internalDateMs = m.data.internalDate
      ? Number(m.data.internalDate)
      : Date.parse(dateHeader);

    const { textPlain, textHtml } = extractBodies(payload);

    // 添付保存
    const attachmentsGcs = [];
    const allParts = flattenParts(payload?.parts || []);
    for (const part of allParts) {
      const isAttachment =
        part?.filename && part.filename.length > 0 && part.body?.attachmentId;
      if (isAttachment) {
        const path = await saveAttachmentToGCS(emailAddress, messageId, part);
        if (path) attachmentsGcs.push(path);
      }
    }

    // Firestore 保存（冪等：messageId を docId に）
    await db.collection("messages").doc(messageId).set(
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
        attachments: attachmentsGcs,
        gcsBucket: GCS_BUCKET,
        createdAt: Date.now(),
      },
      { merge: true }
    );

    console.log(
      `Saved message ${messageId} (attachments: ${attachmentsGcs.length})`
    );
  }

  // 最新 historyId を更新
  await userDoc.set(
    { lastHistoryId: historyId, updatedAt: Date.now() },
    { merge: true }
  );
}

// ====== ルーティング ======
app.get("/", (_req, res) => res.status(200).send("ok"));

app.post("/gmail/push", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.data) {
      console.warn("No Pub/Sub message.data");
      return res.status(204).send(); // 204: リトライ抑制
    }

    const decoded = Buffer.from(msg.data, "base64").toString("utf8");
    console.log("📩 Gmail Push Notification:", decoded);

    // Gmail Push の data は {"emailAddress":"...","historyId":"..."}
    const { emailAddress, historyId } = JSON.parse(decoded);

    if (!emailAddress || !historyId) {
      console.warn("Missing emailAddress/historyId");
      return res.status(204).send();
    }

    await handleHistory(emailAddress, historyId);
    return res.status(200).send();
  } catch (err) {
    console.error("Push handler error:", err);
    // 非2xx で Pub/Sub が最大7日間リトライ
    return res.status(500).send();
  }
});

// ====== サーバ起動 ======
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`listening on ${port}`);
});
