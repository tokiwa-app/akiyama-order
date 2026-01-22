// ingest.js
/**
 * 1) Gmail → Supabase ingest
 *
 * ★簡単版（n8n方式）:
 * - 本文は Gmail "raw" を mailparser でパースして取得する
 *   - body_text = parsed.text
 *   - body_html = parsed.html || parsed.textAsHtml
 * - 添付は今まで通り "full" payload の attachmentId を使って saveAttachmentToGCS（既存のまま）
 *
 * これで:
 * - body_html が空になりがちな問題（attachmentId化）を気にしなくて済む
 * - text/html が無いメールでも textAsHtml が作られるので PDF化もしやすい
 */

import { simpleParser } from "mailparser";

function b64UrlToBuffer(b64url) {
  const b64 = (b64url || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64");
}

export function registerIngestRoutes(app, deps) {
  const {
    FAX_SENDER,
    LOOKBACK_MINUTES,
    supabase,
    getGmail,
    getHeader,
    flattenParts,
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
      const minutes =
        Number.isFinite(minutesParam) && minutesParam > 0
          ? Math.floor(minutesParam)
          : LOOKBACK_MINUTES;

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

          // 既存ならスキップ
          const { data: existsRow, error: exErr } = await supabase
            .from("messages")
            .select("id")
            .eq("id", id)
            .maybeSingle();
          if (exErr) throw exErr;
          if (existsRow) continue;

          // 1) full（ヘッダ/添付探索用）
          const full = await gmail.users.messages.get({
            userId: "me",
            id,
            format: "full",
          });

          const payload = full.data.payload;
          const headers = payload?.headers || [];
          const subject = getHeader(headers, "Subject");
          const from = getHeader(headers, "From");
          const to = getHeader(headers, "To");
          const dateHdr = getHeader(headers, "Date");

          const internalDateMs = full.data.internalDate
            ? Number(full.data.internalDate)
            : Date.parse(dateHdr);

          const receivedAt = internalDateMs ? new Date(internalDateMs) : new Date();
          const receivedAtIso = receivedAt.toISOString();

          const addrMatch = (from || "").match(/<([^>]+)>/);
          const fromEmail = (addrMatch ? addrMatch[1] : (from || ""))
            .trim()
            .toLowerCase();
          const messageType = FAX_SENDER && fromEmail === FAX_SENDER ? "fax" : "mail";

          const threadId = full.data.threadId || id;
          const managementNo = `gmail_${threadId}`;

          // 2) raw（本文を確実に取る：n8n方式）
          const rawRes = await gmail.users.messages.get({
            userId: "me",
            id,
            format: "raw",
          });

          const emlBuf = b64UrlToBuffer(rawRes.data.raw);
          const parsed = await simpleParser(emlBuf);

          const bodyText = parsed.text || "";
          // ★ここがポイント：HTMLメールなら parsed.html、無いなら parsed.textAsHtml
          const bodyHtml = parsed.html || parsed.textAsHtml || null;

          // cases 先に作る（未特定は 0 / 未設定）
          const caseId = await upsertCaseByManagementNo(
            managementNo,
            0,
            "未設定",
            subject ?? null,
            receivedAtIso
          );

          // messages insert（case_id必須）
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

              body_text: bodyText,
              body_html: bodyHtml,

              body_type: messageType === "fax" ? "fax_raw" : "mail_raw",
              processed_at: null,
              processing_at: null,
              ocr_status: "pending",

              customer_id: 0,
              customer_name: "未設定",
            });

          if (insMsgErr) throw insMsgErr;

          // 3) 添付保存（既存ロジックそのまま：GCS）
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
              const { error: attErr } = await supabase
                .from("message_attachments")
                .insert(rows);
              if (attErr) console.error("insert message_attachments error:", attErr);
            }
          } else {
            // fax は main_pdf_path を添付PDFに
            const pdf = attachments.find(
              (p) => typeof p === "string" && p.toLowerCase().endsWith(".pdf")
            );
            const any = attachments.find((p) => typeof p === "string");
            const mainPdfPath = pdf || any || null;

            if (mainPdfPath) {
              await supabase
                .from("messages")
                .update({ main_pdf_path: mainPdfPath })
                .eq("id", id);

              const row = {
                case_id: caseId,
                message_id: id,
                gcs_path: mainPdfPath,
                file_name:
                  typeof mainPdfPath === "string"
                    ? mainPdfPath.split("/").pop() || null
                    : null,
                mime_type: "application/pdf",
                file_type: "fax_original",
                thumbnail_path: null,
              };
              const { error: mainErr } = await supabase
                .from("message_main_pdf_files")
                .insert(row);
              if (mainErr) console.error("insert message_main_pdf_files error:", mainErr);
            }
          }

          ingested++;
        }
      } while (pageToken);

      res
        .status(200)
        .send(
          `OK seen=${seen} ingested=${ingested} minutes=${minutes} in ${
            Date.now() - started
          }ms`
        );
    } catch (e) {
      console.error("/gmail/poll error:", e);
      res.status(500).send("error");
    }
  });
}
