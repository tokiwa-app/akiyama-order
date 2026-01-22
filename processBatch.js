// processBatch.js
/**
 * 2) pending を少量処理
 * - messages から未処理を取ってロック
 * - fax: message_main_pdf_files からPDF取得 → OCR → 顧客特定 → cases/messages 更新
 * - mail: body_text で顧客特定（必要なら）
 */
export function registerProcessBatchRoutes(app, deps) {
  const {
    supabase,
    runOcrForAttachments,
    detectCustomerFromMaster,
  } = deps;

  app.post("/gmail/process-batch", async (req, res) => {
    const started = Date.now();
    const limit = Math.min(Number(req.query?.limit || 2), 10);
    const lockMinutes = 10;

    try {
      const { data: rows, error: selErr } = await supabase
        .from("messages")
        .select("id, case_id, message_type, subject, body_text, received_at")
        .is("processed_at", null)
        .or(`processing_at.is.null,processing_at.lt.${new Date(Date.now() - lockMinutes * 60 * 1000).toISOString()}`)
        .order("received_at", { ascending: false })
        .limit(limit);

      if (selErr) throw selErr;

      let processed = 0;

      for (const m of rows || []) {
        const nowIso = new Date().toISOString();
        const { error: lockErr } = await supabase
          .from("messages")
          .update({ processing_at: nowIso, ocr_status: "processing", ocr_error: null })
          .eq("id", m.id)
          .is("processed_at", null);

        if (lockErr) {
          console.error("lockErr:", lockErr);
          continue;
        }

        try {
          let attachments = [];
          let mainPdfPath = null;

          if (m.message_type === "fax") {
            const { data: pdfRows, error: pdfErr } = await supabase
              .from("message_main_pdf_files")
              .select("gcs_path")
              .eq("message_id", m.id)
              .order("created_at", { ascending: true })
              .limit(10);

            if (pdfErr) throw pdfErr;
            attachments = (pdfRows || []).map((r) => r.gcs_path).filter(Boolean);
            mainPdfPath = attachments[0] || null;
          } else {
            const { data: attRows } = await supabase
              .from("message_attachments")
              .select("gcs_path")
              .eq("message_id", m.id);
            attachments = (attRows || []).map((r) => r.gcs_path).filter(Boolean);
          }

          let ocrText = "";
          let customer = null;

          if (m.message_type === "fax") {
            ocrText = await runOcrForAttachments(attachments);
            const head = String(ocrText || "").slice(0, 200);
            customer = (head && (await detectCustomerFromMaster(head))) || (await detectCustomerFromMaster(ocrText));
          } else {
            const head = String(m.body_text || "").slice(0, 300);
            customer = (head && (await detectCustomerFromMaster(head))) || (await detectCustomerFromMaster(m.body_text || ""));
          }

          // cases 更新（顧客が取れた場合）
          if (m.case_id && (customer?.id || customer?.name)) {
            await supabase
              .from("cases")
              .update({
                customer_id: customer?.id ?? null,
                customer_name: customer?.name ?? null,
                latest_message_at: m.received_at ?? null,
                title: m.subject ?? null,
              })
              .eq("id", m.case_id);
          }

          const { error: updErr } = await supabase
            .from("messages")
            .update({
              main_pdf_path: mainPdfPath ?? null,
              ocr_text: ocrText,
              customer_id: customer?.id ?? null,
              customer_name: customer?.name ?? null,
              ocr_status: "done",
              processed_at: new Date().toISOString(),
              processing_at: null,
            })
            .eq("id", m.id);

          if (updErr) throw updErr;

          processed++;
        } catch (e) {
          console.error("process one error:", m.id, e);

          await supabase
            .from("messages")
            .update({
              ocr_status: "error",
              ocr_error: e?.message || String(e),
              processing_at: null,
            })
            .eq("id", m.id);
        }
      }

      res.status(200).send(`OK processed=${processed} picked=${(rows || []).length} in ${Date.now() - started}ms`);
    } catch (e) {
      console.error("/gmail/process-batch error:", e);
      res.status(500).send("error");
    }
  });
}
