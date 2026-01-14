// supabaseSync.js
import { supabase } from "./supabaseClient.js";

/**
 * Firestore messages → Supabase に同期
 *
 * この処理は必ず try/catch で安全に動き、afterProcess を止めない。
 */
export async function mirrorMessageToSupabase({
  messageId,
  data,
  managementNo,
  customer,
}) {
  try {
    if (!supabase) {
      console.warn("Supabase client not ready");
      return;
    }

    // ---------- 1) CASE（案件）取得 or 生成 ----------
    const { data: caseRow, error: caseSelectErr } = await supabase
      .from("cases")
      .select("id")
      .eq("management_no", managementNo)
      .maybeSingle();

    if (caseSelectErr) {
      console.error("Supabase case select error:", caseSelectErr);
      return;
    }

    let caseId;
    if (caseRow) {
      caseId = caseRow.id;
    } else {
      const { data: inserted, error: caseInsertErr } = await supabase
        .from("cases")
        .insert({
          management_no: managementNo,
          customer_id: customer?.id || null,
          customer_name: customer?.name || null,
          title: data.subject || null,
          latest_message_at: new Date(),
        })
        .select()
        .maybeSingle();

      if (caseInsertErr) {
        console.error("Supabase case insert error:", caseInsertErr);
        return;
      }
      caseId = inserted.id;
    }

    // ---------- 2) MESSAGES：upsert ----------
    const internalDate = data.internalDate
      ? new Date(data.internalDate)
      : new Date();

    const { error: msgErr } = await supabase.from("messages").upsert(
      {
        id: messageId,
        case_id: caseId,
        message_type: data.messageType,
        subject: data.subject,
        from_email: data.from,
        to_email: data.to,
        snippet: data.snippet,
        received_at: internalDate,
      },
      { onConflict: "id" }
    );

    if (msgErr) console.error("Supabase message upsert error:", msgErr);

    // ---------- 3) ATTACHMENTS：insert ----------
    const attachments = data.attachments || [];
    if (attachments.length > 0) {
      const rows = attachments.map((p) => ({
        case_id: caseId,
        message_id: messageId,
        gcs_path: p,
        file_name: p.split("/").pop(),
      }));

      const { error: attErr } = await supabase
        .from("message_attachments")
        .insert(rows);

      if (attErr) console.error("Supabase attachment insert error:", attErr);
    }

    console.log(`🔁 Supabase sync OK messageId=${messageId} caseId=${caseId}`);
  } catch (e) {
    console.error("Supabase sync exception:", e);
  }
}
