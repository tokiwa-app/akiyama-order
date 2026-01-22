// supabaseSync.js
import { supabase } from "./supabaseClient.js";

function stripHtmlTags(html = "") {
  return html.replace(/<[^>]*>/g, " ");
}

function getReceivedAt(data) {
  if (typeof data.internalDate === "number") return new Date(data.internalDate);
  if (data.receivedAt?.toDate) return data.receivedAt.toDate();
  if (data.receivedAt instanceof Date) return data.receivedAt;
  return new Date();
}

async function getMessageDoc(firestore, messageId) {
  const snap = await firestore.collection("messages").doc(messageId).get();
  if (!snap.exists) return null;
  return snap.data();
}

async function ensureCaseByManagementNo(
  managementNo,
  customerId,
  customerName,
  title,
  receivedAtIso
) {
  const { data: existing, error: selectErr } = await supabase
    .from("cases")
    .select("id")
    .eq("management_no", managementNo)
    .maybeSingle();

  if (selectErr) throw selectErr;

  if (existing) {
    const { error: updateErr } = await supabase
      .from("cases")
      .update({
        customer_id: customerId,
        customer_name: customerName,
        latest_message_at: receivedAtIso,
        title: title ?? null,
      })
      .eq("id", existing.id);

    if (updateErr) console.error("Supabase update cases error:", updateErr);
    return existing.id;
  }

  const { data: inserted, error: insertErr } = await supabase
    .from("cases")
    .insert({
      management_no: managementNo,
      customer_id: customerId,
      customer_name: customerName,
      title: title ?? null,
      latest_message_at: receivedAtIso,
    })
    .select()
    .single();

  if (insertErr) throw insertErr;
  return inserted.id;
}

async function migrateCaseManagementNo(oldManagementNo, newManagementNo) {
  if (!oldManagementNo || !newManagementNo || oldManagementNo === newManagementNo)
    return;

  const { data: oldCase, error: oldErr } = await supabase
    .from("cases")
    .select("id")
    .eq("management_no", oldManagementNo)
    .maybeSingle();

  if (oldErr) {
    console.error("Supabase select old case error:", oldErr);
    return;
  }
  if (!oldCase) return;

  const { data: newCase, error: newErr } = await supabase
    .from("cases")
    .select("id")
    .eq("management_no", newManagementNo)
    .maybeSingle();

  if (newErr) {
    console.error("Supabase select new case error:", newErr);
    return;
  }
  if (newCase) return; // 衝突回避

  const { error: updErr } = await supabase
    .from("cases")
    .update({ management_no: newManagementNo })
    .eq("id", oldCase.id);

  if (updErr) console.error("Supabase migrate case management_no error:", updErr);
}

/**
 * ✅ afterProcess不要の最低限同期
 * - cases.management_no は一旦 messageId を使う（仮case）
 * - messages は upsert
 * - 添付/PDFは full に任せる
 */
export async function mirrorMessageToSupabaseBasic({ messageId, firestore }) {
  try {
    if (!supabase) {
      console.error("Supabase client is not initialized.");
      return;
    }

    const data = await getMessageDoc(firestore, messageId);
    if (!data) return;

    const receivedAt = getReceivedAt(data);
    const receivedAtIso = receivedAt.toISOString();

    const isFax = data.messageType === "fax";

    let bodyText = "";
    let bodyType = "basic";

    if (!isFax) {
      if (data.textPlain) bodyText = data.textPlain;
      else if (data.textHtml) bodyText = stripHtmlTags(data.textHtml);
      else bodyText = "";
      bodyType = "mail_raw";
    } else {
      bodyText = data.snippet ?? "";
      bodyType = "fax_basic";
    }

    const tempManagementNo = messageId;
    console.log("🧩 basic sync start", {
      messageId,
      tempManagementNo,
      messageType: data.messageType,
    });

    const customerId = data.customerId ?? null;
    const customerName = data.customerName ?? null;

    const caseId = await ensureCaseByManagementNo(
      tempManagementNo,
      customerId,
      customerName,
      data.subject ?? null,
      receivedAtIso
    );

    const upsertPayload = {
      id: messageId,
      case_id: caseId,
      message_type: data.messageType ?? null,
      subject: data.subject ?? null,
      from_email: data.from ?? null,
      to_email: data.to ?? null,
      received_at: receivedAtIso,
      snippet: data.snippet ?? null,
      main_pdf_path: null,
      body_text: bodyText,
      body_type: bodyType,
    };

    const { error: msgErr } = await supabase
      .from("messages")
      .upsert(upsertPayload, { onConflict: "id" });

    if (msgErr) console.error("Supabase upsert messages error:", msgErr);

    console.log(`✅ basic sync OK messageId=${messageId} caseId=${caseId}`);
  } catch (e) {
    console.error("mirrorMessageToSupabaseBasic exception:", e);
    throw e;
  }
}

/**
 * ✅ afterProcess成功後の完成同期
 * - 仮case (management_no=messageId) を本物 managementNo に移行
 * - messages / 添付 / main pdf を整合させる
 * - 添付/PDFは delete→insert で冪等化
 */
export async function mirrorMessageToSupabaseFull({
  messageId,
  firestore,
  managementNo,
  customer,
  mainPdfPath,
  mainPdfThumbnailPath,
  fullOcrText,
}) {
  try {
    if (!supabase) {
      console.error("Supabase client is not initialized.");
      return;
    }

    const data = await getMessageDoc(firestore, messageId);
    if (!data) return;

    const receivedAt = getReceivedAt(data);
    const receivedAtIso = receivedAt.toISOString();

    const isFax = data.messageType === "fax";
    const attachments = Array.isArray(data.attachments) ? data.attachments : [];

    const customerId = customer?.id ?? data.customerId ?? null;
    const customerName = customer?.name ?? data.customerName ?? null;

    let bodyText = "";
    let bodyType = "";

    if (isFax) {
      bodyText = fullOcrText ?? data.ocr?.fullText ?? "";
      bodyType = "fax_ocr";
    } else {
      if (data.textPlain) bodyText = data.textPlain;
      else if (data.textHtml) bodyText = stripHtmlTags(data.textHtml);
      else bodyText = "";
      bodyType = "mail_raw";
    }

    const finalManagementNo = managementNo || data.managementNo;
    if (!finalManagementNo) {
      console.warn("full sync skipped: managementNo missing", { messageId });
      return;
    }

    console.log("🔁 full sync start", {
      messageId,
      finalManagementNo,
      messageType: data.messageType,
    });

    await migrateCaseManagementNo(messageId, finalManagementNo);

    const caseId = await ensureCaseByManagementNo(
      finalManagementNo,
      customerId,
      customerName,
      data.subject ?? null,
      receivedAtIso
    );

    const finalMainPdfPath =
      mainPdfPath ?? data.mainPdfPath ?? data.main_pdf_path ?? null;

    const upsertPayload = {
      id: messageId,
      case_id: caseId,
      message_type: data.messageType ?? null,
      subject: data.subject ?? null,
      from_email: data.from ?? null,
      to_email: data.to ?? null,
      received_at: receivedAtIso,
      snippet: data.snippet ?? null,
      main_pdf_path: finalMainPdfPath ?? null,
      body_text: bodyText,
      body_type: bodyType,
    };

    const { error: msgErr } = await supabase
      .from("messages")
      .upsert(upsertPayload, { onConflict: "id" });

    if (msgErr) console.error("Supabase upsert messages error:", msgErr);

    // 3) message_attachments（mailのみ）: delete → insert
    if (!isFax) {
      const { error: delErr } = await supabase
        .from("message_attachments")
        .delete()
        .eq("message_id", messageId);
      if (delErr) console.error("Supabase delete message_attachments error:", delErr);

      const rows = attachments.map((p) => ({
        case_id: caseId,
        message_id: messageId,
        gcs_path: p,
        file_name: typeof p === "string" ? p.split("/").pop() || null : null,
        mime_type: null,
      }));

      if (rows.length) {
        const { error: insErr } = await supabase
          .from("message_attachments")
          .insert(rows);
        if (insErr) console.error("Supabase insert message_attachments error:", insErr);
      }
    }

    // 4) message_main_pdf_files: delete → insert
    const { error: delMainErr } = await supabase
      .from("message_main_pdf_files")
      .delete()
      .eq("message_id", messageId);
    if (delMainErr) console.error("Supabase delete message_main_pdf_files error:", delMainErr);

    if (finalMainPdfPath) {
      const row = {
        case_id: caseId,
        message_id: messageId,
        gcs_path: finalMainPdfPath,
        file_name:
          typeof finalMainPdfPath === "string"
            ? finalMainPdfPath.split("/").pop() || null
            : null,
        mime_type: "application/pdf",
        file_type: isFax ? "fax_original" : "mail_rendered",
        thumbnail_path: mainPdfThumbnailPath ?? null,
      };

      const { error: mainErr } = await supabase
        .from("message_main_pdf_files")
        .insert(row);

      if (mainErr) console.error("Supabase insert message_main_pdf_files error:", mainErr);
    }

    console.log(
      `✅ full sync OK messageId=${messageId} managementNo=${finalManagementNo} caseId=${caseId}`
    );
  } catch (e) {
    console.error("mirrorMessageToSupabaseFull exception:", e);
    throw e;
  }
}
