// pdfUtil.js
import fs from "fs/promises";
import puppeteer from "puppeteer";
import { Storage } from "@google-cloud/storage";

const storage = new Storage();

/**
 * HTML → PDF → JPG thumbnail (幅300px想定)
 * - メール用
 */
export async function renderMailHtmlToPdfAndThumbnail({ bucket, messageId, html }) {
  const safeId = sanitizeId(messageId);
  const pdfTmp = `/tmp/mail-${safeId}.pdf`;
  const jpgTmp = `/tmp/mail-${safeId}.jpg`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });

    // PDF作成
    await page.pdf({
      path: pdfTmp,
      format: "A4",
      printBackground: true,
    });

    // サムネ生成（幅300px）
    await page.setViewport({ width: 1024, height: 1400 });
    const zoom = 300 / 1024;

    await page.evaluate((zoomFactor) => {
      document.body.style.zoom = zoomFactor;
    }, zoom);

    await page.screenshot({
      path: jpgTmp,
      type: "jpeg",
      quality: 80,
      fullPage: true,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  // GCS アップロード
  const pdfObjectPath = `mail_rendered/${safeId}.pdf`;
  const jpgObjectPath = `mail_thumbs/${safeId}.jpg`;

  const pdfBuf = await fs.readFile(pdfTmp);
  const jpgBuf = await fs.readFile(jpgTmp);

  await bucket.file(pdfObjectPath).save(pdfBuf, {
    resumable: false,
    contentType: "application/pdf",
  });

  await bucket.file(jpgObjectPath).save(jpgBuf, {
    resumable: false,
    contentType: "image/jpeg",
  });

  return {
    pdfPath: `gs://${bucket.name}/${pdfObjectPath}`,
    thumbnailPath: `gs://${bucket.name}/${jpgObjectPath}`,
  };
}

/**
 * FAX PDF → JPG thumbnail (幅300px想定)
 * - FAX用（gs:// パス前提）
 */
export async function renderPdfToThumbnail({ bucket, pdfGsUri, messageId }) {
  const info = parseGsUri(pdfGsUri);
  if (!info) throw new Error("Invalid GCS URI: " + pdfGsUri);

  const safeId = sanitizeId(messageId);
  const localPdf = `/tmp/fax-${safeId}.pdf`;
  const localJpg = `/tmp/fax-${safeId}.jpg`;

  // PDF ダウンロード
  await storage.bucket(info.bucket).file(info.path).download({ destination: localPdf });

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();

    // PDF は networkidle0 ではまりやすいので load にして少し待つ
    await page.goto(`file://${localPdf}`, { waitUntil: "load" });
    await page.waitForTimeout(1000);

    await page.setViewport({ width: 1024, height: 1400 });
    const zoom = 300 / 1024;

    await page.evaluate((zoomFactor) => {
      document.body.style.zoom = zoomFactor;
    }, zoom);

    await page.screenshot({
      path: localJpg,
      type: "jpeg",
      quality: 80,
      fullPage: true,
    });
  } finally {
    await browser.close().catch(() => {});
  }

  const thumbObjectPath = `fax_thumbs/${safeId}.jpg`;
  const jpgBuf = await fs.readFile(localJpg);

  await bucket.file(thumbObjectPath).save(jpgBuf, {
    resumable: false,
    contentType: "image/jpeg",
  });

  return `gs://${bucket.name}/${thumbObjectPath}`;
}

/* ====== helper ====== */

function sanitizeId(id) {
  return String(id || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 100);
}

function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}
