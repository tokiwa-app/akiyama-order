// supabaseSync.js
import { supabase } from "./supabaseClient.js";

/**
 * とりあえず managementNo を cases に保存するだけの超シンプル版
 */
export async function mirrorMessageToSupabase({
  messageId,      // 今は使わないけど将来のために残す
  data,
  managementNo,
  customer,       // 今は使わない
}) {
  try {
    if (!supabase) {
      console.warn("Supabase client not initialized");
      return;
    }

    // 必須：management_no と title だけ
    const { error } = await supabase.from("cases").insert({
      management_no: managementNo,
      title: data.subject || null,
    });

    if (error) {
      console.error("Supabase simple insert error:", error);
    } else {
      console.log("✅ Supabase simple insert OK:", managementNo);
    }
  } catch (e) {
    console.error("Supabase simple insert exception:", e);
  }
}
