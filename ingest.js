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
 */
export function registerIngestRoutes(app, deps) {
  const {
    FAX_SENDER,
    LOOKBACK_MINUTES,
    supabase,
    getGmail,
    getHeader,
    flattenParts,
    extractBodies,
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

              // ★変更点：mailのHTMLを捨てない
              // - plainは body_text
              // - htmlは body_html
              body_text: textPlain || "",
              body_html: textHtml || null,

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
}
