// supabaseSync.js
import { supabase } from "./supabaseClient.js";

/**
 * Firestore messages ドキュメント → Supabase に同期
 *
 * 対応テーブル:
 *  - cases
 *  - messages
 *  - message_attachments
 *  - message_main_pdf_files（FAXのPDFをメイン扱い）
 */
export async function mirrorMessageToSupabase({
  messageId,
  data,
  managementNo,
  customer,
}) {
  try {
    if (!supabase) {
      console.error("Supabase client is not initialized.");
      return;
    }

    console.log("🔁 mirrorMessageToSupabase start", {
      messageId,
      managementNo,
      messageType: data.messageType,
    });

    // ---------- 受信日時 ----------
    let receivedAt;
    if (typeof data.internalDate === "number") {
      receivedAt = new Date(data.internalDate);
    } else if (data.receivedAt?.toDate) {
      // Firestore Timestamp
      receivedAt = data.receivedAt.toDate();
    } else if (data.receivedAt instanceof Date) {
      receivedAt = data.receivedAt;
    } else {
      receivedAt = new Date();
    }

    const customerId = customer?.id ?? data.customerId ?? null;
    const customerName = customer?.name ?? data.customerName ?? null;

    // ======================================================
    // 1) cases: 案件（management_no 単位）
    // ======================================================
    let caseId = null;

    {
      const { data: existing, error: selectErr } = await supabase
        .from("cases")
        .select("id")
        .eq("management_no", managementNo)
        .maybeSingle();

      if (selectErr) {
        console.error("Supabase select cases error:", selectErr);
        return;
      }

      if (existing) {
        caseId = existing.id;

        // 取引先・最新日時を更新しておく
        const { error: updateErr } = await supabase
          .from("cases")
          .update({
            customer_id: customerId,
            customer_name: customerName,
            latest_message_at: receivedAt.toISOString(),
          })
          .eq("id", caseId);

        if (updateErr) {
          console.error("Supabase update cases error:", updateErr);
        }
      } else {
        const { data: inserted, error: insertErr } = await supabase
          .from("cases")
          .insert({
            management_no: managementNo,
            customer_id: customerId,
            customer_name: customerName,
            title: data.subject ?? null,
            latest_message_at: receivedAt.toISOString(),
          })
          .select()
          .single();

        if (insertErr) {
          console.error("Supabase insert cases error:", insertErr);
          return;
        }

        caseId = inserted.id;
      }
    }

    if (!caseId) {
      console.error("caseId is null. Abort sync.");
      return;
    }

    // ======================================================
    // 2) messages: メール/FAX 1通
    // ======================================================
    let mainPdfPath = null;

    // FAX の場合、添付PDFをメイン扱いする（最初の1つを main_pdf_path に）
    const attachments = Array.isArray(data.attachments)
      ? data.attachments
      : [];

    const pdfAttachments = attachments.filter(
      (p) =>
        typeof p === "string" && p.toLowerCase().endsWith(".pdf")
    );

    if (data.messageType === "fax" && pdfAttachments.length > 0) {
      mainPdfPath = pdfAttachments[0];
    }

    {
      const { error: msgErr } = await supabase.from("messages").upsert(
        {
          id: messageId,
          case_id: caseId,
          message_type: data.messageType ?? null, // 'fax' or 'mail'
          subject: data.subject ?? null,
          from_email: data.from ?? null,
          to_email: data.to ?? null,
          received_at: receivedAt.toISOString(),
          snippet: data.snippet ?? null,
          main_pdf_path: mainPdfPath, // FAXのときだけ今は入る
        },
        { onConflict: "id" }
      );

      if (msgErr) {
        console.error("Supabase upsert messages error:", msgErr);
      }
    }

    // ======================================================
    // 3) message_attachments: 雑多な添付（全部）
    // ======================================================
    if (attachments.length > 0) {
      const rows = attachments.map((path) => ({
        case_id: caseId,
        message_id: messageId,
        gcs_path: path,
        file_name:
          typeof path === "string" ? path.split("/").pop() || null : null,
        mime_type: null, // 必要になったら GCS から取る or Firestoreに持たせる
      }));

      const { error: attErr } = await supabase
        .from("message_attachments")
        .insert(rows);

      if (attErr) {
        console.error(
          "Supabase insert message_attachments error:",
          attErr
        );
      }
    }

    // ======================================================
    // 4) message_main_pdf_files: メインPDF（FAXのPDFだけ今は対象）
    // ======================================================
    if (data.messageType === "fax" && pdfAttachments.length > 0) {
      const mainRows = pdfAttachments.map((path, index) => ({
        case_id: caseId,
        message_id: messageId,
        gcs_path: path,
        thumbnail_path: null, // サムネ生成を入れたらここに設定
        file_name:
          typeof path === "string" ? path.split("/").pop() || null : null,
        mime_type: "application/pdf",
        file_type: index === 0 ? "fax_original" : "fax_additional",
      }));

      const { error: mainErr } = await supabase
        .from("message_main_pdf_files")
        .insert(mainRows);

      if (mainErr) {
        console.error(
          "Supabase insert message_main_pdf_files error:",
          mainErr
        );
      }
    }

    console.log(
      `✅ Supabase sync OK messageId=${messageId} managementNo=${managementNo} caseId=${caseId}`
    );
  } catch (e) {
    console.error("mirrorMessageToSupabase exception:", e);
  }
}
