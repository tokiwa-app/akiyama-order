// index.js ーーー 起動を確実にするためのパッチ版
import express from "express";

console.log("[BOOT] process start");

const app = express();
app.use(express.json());

// 起動直後に必ずログを出す
app.get("/", (_req, res) => {
  res.status(200).send("ok");
});

// 遅延ロード用（初回アクセスまで heavy deps を読み込まない）
let gmailReady = false;
let getGmailClient, handleHistory;

async function lazyInit() {
  if (gmailReady) return;
  console.log("[BOOT] lazyInit start");

  // ←↓↓ ここで重い依存を読み込む ↓↓→
  const { google } = await import("googleapis");
  const { Firestore } = await import("@google-cloud/firestore");
  const { Storage } = await import("@google-cloud/storage");

  const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
  const GCS_BUCKET = process.env.GCS_BUCKET;

  const db = new Firestore(
    FIREBASE_PROJECT_ID ? { projectId: FIREBASE_PROJECT_ID } : {}
  );
  const storage = new Storage();
  const bucket = storage.bucket(GCS_BUCKET);

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
    let textPlain = "", textHtml = "";
    if (!payload) return { textPlain, textHtml };
    const parts = payload.parts ? flattenParts(payload.parts) : [payload];
    for (const p of parts) {
      if (p.mimeType === "text/plain" && p.body?.data) textPlain += b64UrlDecode(p.body.data);
      if (p.mimeType === "text/html"  && p.body?.data) textHtml  += b64UrlDecode(p.body.data);
    }
    return { textPlain, textHtml };
  }

  async function getGmail() {
    // OAuth 環境変数があれば3LO、無ければ ADC（Cloud Run 実行SA）
    const { OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REFRESH_TOKEN } = process.env;
    if (OAUTH_CLIENT_ID && OAUTH_CLIENT_SECRET && OAUTH_REFRESH_TOKEN) {
      const oAuth2Client = new google.auth.OAuth2(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET);
      oAuth2Client.setCredentials({ refresh_token: OAUTH_REFRESH_TOKEN });
      return google.gmail({ version: "v1", auth: oAuth2Client });
    }
    const auth = new google.auth.GoogleAuth({ scopes: ["https://www.googleapis.com/auth/gmail.readonly"] });
    const client = await auth.getClient();
    return google.gmail({ version: "v1", auth: client });
  }

  async function saveAttachmentToGCS(userEmail, messageId, part) {
    const attachId = part?.body?.attachmentId;
    if (!attachId) return null;
    const gmail = await getGmail();
    const res = await gmail.users.messages.attachments.get({ userId: "me", messageId, id: attachId });
    const bytes = Buffer.from(res.data.data, "base64");
    const filename = part.filename || `attachment-${attachId}`;
    const objectPath = `gmail/${encodeURIComponent(userEmail)}/${messageId}/${filename}`;
    await bucket.file(objectPath).save(bytes, { resumable: false, metadata: { contentType: part.mimeType || "application/octet-stream" } });
    return `gs://${GCS_BUCKET}/${objectPath}`;
  }

  async function _handleHistory(emailAddress, historyId) {
    const gmail = await getGmail();

    const userDoc = db.collection("gmail_users").doc(emailAddress);
    const snap = await userDoc.get();
    const lastHistoryId = snap.exists ? snap.data().lastHistoryId : null;

    if (!lastHistoryId) {
      await userDoc.set({ lastHistoryId: historyId, updatedAt: Date.now() }, { merge: true });
      console.log(`[INIT] historyId=${historyId} saved for ${emailAddress}`);
      return;
    }

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
        for (const ma of h.messagesAdded || []) if (ma.message?.id) newIds.add(ma.message.id);
      }
    } while (pageToken);

    for (const id of newIds) {
      const m = await gmail.users.messages.get({ userId: "me", id, format: "full" });
      const payload = m.data.payload;
      const headers = payload?.headers || [];
      const subject = getHeader(headers, "Subject");
      const from    = getHeader(headers, "From");
      const to      = getHeader(headers, "To");
      const cc      = getHeader(headers, "Cc");
      const dateHdr = getHeader(headers, "Date");
      const internalDateMs = m.data.internalDate ? Number(m.data.internalDate) : Date.parse(dateHdr);
      const { textPlain, textHtml } = extractBodies(payload);

      const attachments = [];
      const parts = flattenParts(payload?.parts || []);
      for (const p of parts) {
        if (p?.filename && p.body?.attachmentId) {
          const path = await saveAttachmentToGCS(emailAddress, id, p);
          if (path) attachments.push(path);
        }
      }

      await db.collection("messages").doc(id).set({
        user: emailAddress,
        threadId: m.data.threadId,
        internalDate: internalDateMs || null,
        receivedAt: internalDateMs ? new Date(internalDateMs) : new Date(),
        from, to, cc, subject,
        snippet: m.data.snippet || "",
        textPlain, textHtml,
        labels: m.data.labelIds || [],
        attachments,
        gcsBucket: process.env.GCS_BUCKET || null,
        createdAt: Date.now(),
      }, { merge: true });
    }

    await userDoc.set({ lastHistoryId: historyId, updatedAt: Date.now() }, { merge: true });
  }

  getGmailClient = getGmail;
  handleHistory = _handleHistory;

  gmailReady = true;
  console.log("[BOOT] lazyInit done");
}

// Push 受信（ここで初回だけ heavy deps を読み込む）
app.post("/gmail/push", async (req, res) => {
  try {
    await lazyInit();

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

// グローバル例外で必ずログを吐く
process.on("unhandledRejection", (e) => console.error("[FATAL] unhandledRejection:", e));
process.on("uncaughtException", (e) => console.error("[FATAL] uncaughtException:", e));

const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => {
  console.log(`[BOOT] listening on ${port}`);
});
