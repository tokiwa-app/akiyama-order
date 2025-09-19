// index.js
import express from "express";
import {google} from "googleapis";
import {Firestore} from "@google-cloud/firestore";
import {Storage} from "@google-cloud/storage";

const app = express();
app.use(express.json());

const PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const BUCKET = process.env.GCS_BUCKET;

// ===== Firestore / Storage =====
const db = new Firestore({ projectId: PROJECT_ID });
const storage = new Storage();
const bucket = storage.bucket(BUCKET);

// ===== Gmail 認証ヘルパ =====
// 1) Workspace（ドメインワイド委任）: サービスアカウントでユーザーを偽装
async function getGmailClientWithDWD(impersonateUser) {
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
  });
  // ユーザー偽装（DWD）する場合
  const client = await auth.getClient();
  // googleapis の DWD は JWT/ImpersonatedCredentials の利用が一般的
  // Cloud Run の SA に DWD 設定済み前提で、`subject` 指定が必要なら下記のように実装
  // ただし googleapis の getClient では subject 指定が直接できないため、
  // 実運用では google-auth-library の Impersonated/JWT を使う実装に置き換えてください。
  // ここでは簡略化のため userId にメールアドレスを渡して使います。
  return google.gmail({version: "v1", auth: client});
}

// 2) 3LO（user consent）方式
async function getGmailClientWithOAuth() {
  const {OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REFRESH_TOKEN} = process.env;
  const oAuth2Client = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET);
  oAuth2Client.setCredentials({ refresh_token: OAUTH_REFRESH_TOKEN });
  return google.gmail({version: "v1", auth: oAuth2Client});
}

// 実際に使うクライアント（環境に合わせて切替）
async function getGmail() {
  if (process.env.GMAIL_IMPERSONATE) {
    return getGmailClientWithDWD(process.env.GMAIL_IMPERSONATE);
  }
  return getGmailClientWithOAuth();
}

// ===== ユーティリティ =====
function getHeader(headers, name) {
  const h = headers?.find(h => h.name?.toLowerCase() === name.toLowerCase());
  return h?.value || "";
}

function decodeBase64UrlSafe(data) {
  // Gmail の body は URL-safe base64
  return Buffer.from(data.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}

async function saveAttachmentToGCS(userEmail, msgId, part) {
  const attachId = part.body?.attachmentId;
  if (!attachId) return null;
  const gmail = await getGmail();
  const res = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId: msgId,
    id: attachId
  });
  const bytes = Buffer.from(res.data.data, "base64");
  const filename = part.filename || `attachment-${attachId}`;
  const objectPath = `gmail/${userEmail}/${msgId}/${filename}`;
  const file = bucket.file(objectPath);
  await file.save(bytes, {
    contentType: part.mimeType || "application/octet-stream",
    resumable: false,
    metadata: { contentType: part.mimeType || "application/octet-stream" }
  });
  return `gs://${BUCKET}/${objectPath}`;
}

function flattenParts(parts) {
  // MIME ツリーをフラットに
  const out = [];
  const stack = [...(parts || [])];
  while (stack.length) {
    const p = stack.shift();
    out.push(p);
    if (p.parts && p.parts.length) stack.push(...p.parts);
  }
  return out;
}

function extractBodies(payload) {
  let textPlain = "";
  let textHtml = "";
  if (!payload) return { textPlain, textHtml };

  const parts = payload.parts ? flattenParts(payload.parts) : [payload];
  for (const p of parts) {
    if (!p.mimeType) continue;
    if (p.mimeType === "text/plain" && p.body?.data) {
      textPlain += decodeBase64UrlSafe(p.body.data);
    } else if (p.mimeType === "text/html" && p.body?.data) {
      textHtml += decodeBase64UrlSafe(p.body.data);
    }
  }
  return { textPlain, textHtml };
}

/
