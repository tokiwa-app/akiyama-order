// ingest.js
/**
 * 1) Gmail → Supabase ingest
 * - 先に cases を作る（messages.case_id NOT NULL 前提）
 * - 添付は GCS 保存
 * - mail: message_attachments に保存（case_id必須）
 * - fax : message_main_pdf_files だけ保存（case_id必須）
 * - OCR/顧客特定はここでやらない
 *
 * 変更点:
 * - mailのHTMLを捨てない: messages.body_html に保存する
 *
 * ★最小変更（今回追加）:
 * - body_html が空になる原因（text/html が body.data ではなく body.attachmentId で来る）に対応
 * - text/plain / text/html で body.data が無いときは attachments.get で回収する
 */

// b64url decode（index.js と同等。depsを増やさないため ingest.js 内に最小追加）
function b64UrlDecode(data) {
  return Buffer.from((data || "").replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}

// ★本文(text/plain, text/html)を “attachmentId でも” 取る（最小追加）
async function extractBodiesAsync({ gmail, messageId, payload, flattenParts }) {
  let textPlain = "";
  let textHtml = "";
  if (!payload) return { textPlain, textHtml };

  const parts = payload.parts ? flattenParts(payload.parts) : [payload];

  async function readPartBody(p) {
    // まず body.data があればそれを使う（従来通り）
    if (p?.body?.data) return b64UrlDecode(p.body.data);

    // data が無く attachmentId の場合（ここが今回の要点）
    const attachId = p?.body?.attachmentId;
    if (!attachId) return "";

    const res = await gmail.users.messages.attachments.get({
      userId: "me",
      messageId,
      id: attachId,
    });

    const data = res?.data?.data || "";
    if (!data) return "";
    return b64UrlDecode(data);
  }

  for (const p of parts) {
    const mt = (p?.mimeType || "").toLowerCase();
    if (mt === "text/plain") textPlain += await readPartBody(p);
    if (mt === "text/html") textHtml += await readPartBody(p);
  }

  return { textPlain, textHtml };
}

export function registerIngestRoutes(app, deps) {
  const {
    FAX_SENDER,
    LOOKBACK_MINUTES,
    supabase,
    getGmail,
    getHeader,
    flattenParts,
    // extractBodies, // ★使わない（attachmentId対応の async を使う）
    saveAttachmentToGCS,
    upsertCaseByManagementNo,
  } = deps;

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

          // ★ここだけ最小変更：本文抽出を async 版へ（attachmentId の本文も回収）
          const { textPlain, textHtml } = await extractBodiesAsync({
            gmail,
            messageId: id,
            payload,
            flattenParts,
          });

          const addrMatch = (from || "").match(/<([^>]+)>/);
          const fromEmail = (addrMatch ? addrMatch[1] : (from || "")).trim().toLowerCase();
          const messageType = FAX_SENDER && fromEmail === FAX_SENDER ? "fax" : "mail";

          const threadId = full.data.threadId || id;
          const managementNo = `gmail_${threadId}`;

          const caseId = await upsertCaseByManagementNo(
            managementNo,
            0, // ★未特定は 0
            "未設定", // ★未設定表示
            subject ?? null,
            receivedAtIso
          );

          // messages 先にinsert（case_id必須）
          const { error: insMsgErr } = await supabase
            .from("messages")
            .insert({
              id,
              case_id: caseId,
              message_type: messageType,
              subject: subject ?? null,
              from_email: from ?? null,
              to_email: to ?? null,
              // cc は現状カラムが無い前提（必要なら追加して保存）
              received_at: receivedAtIso,
              snippet: full.data.snippet ?? null,
              main_pdf_path: null,

              // mail:
              // - plain は body_text
              // - html は body_html
              body_text: textPlain || "",
              body_html: textHtml || null,

              body_type: messageType === "fax" ? "fax_raw" : "mail_raw",
              processed_at: null,
              processing_at: null,
              ocr_status: "pending",

              // 顧客は後段で特定するが、初期値を入れたいならここで
              customer_id: 0,
              customer_name: "未設定",
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
}
