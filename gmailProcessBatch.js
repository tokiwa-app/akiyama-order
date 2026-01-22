import vision from "@google-cloud/vision";
import { Storage } from "@google-cloud/storage";
import { Firestore } from "@google-cloud/firestore";
import { supabase } from "./supabaseClient.js";

/* ================= INIT ================= */
const visionClient = new vision.ImageAnnotatorClient();
const storage = new Storage();
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;

const customerDb = new Firestore(
  FIREBASE_PROJECT_ID
    ? { projectId: FIREBASE_PROJECT_ID, databaseId: "akiyama-system" }
    : { databaseId: "akiyama-system" }
);

/* ================= OCR ================= */
async function runOcr(gcsUri) {
  const [res] = await visionClient.documentTextDetection({
    image: { source: { imageUri: gcsUri } },
  });
  return res.fullTextAnnotation?.text || "";
}

/* ================= CUSTOMER ================= */
async function detectCustomerFromMaster(text) {
  const snap = await customerDb.collection("jsons").doc("Client Search").get();
  if (!snap.exists) return null;

  const matrix = JSON.parse(snap.data().main).tables[0].matrix;
  const header = matrix[0];
  const idIdx = header.indexOf("id");
  const nameIdx = header.indexOf("name");

  const norm = (s) => String(s || "").toLowerCase().replace(/\s+/g, "");
  const t = norm(text);

  for (const row of matrix.slice(1)) {
    if (t.includes(norm(row[nameIdx]))) {
      return { id: row[idIdx], name: row[nameIdx] };
    }
  }
  return null;
}

/* ================= HANDLER ================= */
export default async function gmailProcessBatch(req, res) {
  try {
    const { data: rows } = await supabase
      .from("messages")
      .select("id, case_id")
      .is("processed_at", null)
      .limit(2);

    for (const m of rows || []) {
      const { data: pdfs } = await supabase
        .from("message_main_pdf_files")
        .select("gcs_path")
        .eq("message_id", m.id)
        .limit(1);

      if (!pdfs?.[0]) continue;

      const ocrText = await runOcr(pdfs[0].gcs_path);
      const customer = await detectCustomerFromMaster(ocrText);

      if (customer) {
        await supabase.from("cases").update({
          customer_id: customer.id,
          customer_name: customer.name,
        }).eq("id", m.case_id);
      }

      await supabase.from("messages").update({
        ocr_text: ocrText,
        processed_at: new Date().toISOString(),
        ocr_status: "done",
      }).eq("id", m.id);
    }

    res.status(200).send("ok");
  } catch (e) {
    console.error(e);
    res.status(500).send("error");
  }
}
