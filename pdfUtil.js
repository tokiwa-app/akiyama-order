// pdfUtil.js
import fs from "fs/promises";
import path from "path";
import puppeteer from "puppeteer";

/**
 * HTML → PDF → JPG thumbnail
 */
export async function renderMailHtmlToPdfAndThumbnail({ bucket, messageId, html }) {
  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();

  await page.setContent(html, { waitUntil: "networkidle0" });

  const pdfTmp = `/tmp/${messageId}.pdf`;
  const jpgTmp = `/tmp/${messageId}.jpg`;

  // １）PDF 出力
  await page.pdf({
    path: pdfTmp,
    format: "A4",
    printBackground: true,
  });

  // ２）PDF を開いて JPG に変換
  await page.goto(`file://${pdfTmp}`);
  await page.setViewport({ width: 1024, height: 1400 });
  await page.screenshot({
    path: jpgTmp,
    type: "jpeg",
    quality: 80,
  });

  await browser.close();

  // ３）GCS にアップロード
  const pdfFile = bucket.file(`mail_rendered/${messageId}.pdf`);
  const jpgFile = bucket.file(`mail_thumbs/${messageId}.jpg`);

  await pdfFile.save(await fs.readFile(pdfTmp), { resumable: false, contentType: "application/pdf" });
  await jpgFile.save(await fs.readFile(jpgTmp), { resumable: false, contentType: "image/jpeg" });

  return {
    pdfPath: `gs://${bucket.name}/mail_rendered/${messageId}.pdf`,
    thumbnailPath: `gs://${bucket.name}/mail_thumbs/${messageId}.jpg`,
  };
}


/**
 * PDF → JPG thumbnail（FAX の場合に使用）
 */

export async function renderPdfToThumbnail({ bucket, pdfGsUri, messageId }) {
  const m = pdfGsUri.match(/^gs:\/\/([^/]+)\/(.+)$/);
  if (!m) throw new Error("Invalid GCS URI: " + pdfGsUri);

  const bucketName = m[1];
  const filePath = m[2];
  const localPdf = `/tmp/${messageId}-fax.pdf`;
  const localJpg = `/tmp/${messageId}-fax.jpg`;

  // GCS から PDF をダウンロード
  const gcsBucket = bucket.storage.bucket(bucketName);
  const tempFile = gcsBucket.file(filePath);
  await tempFile.download({ destination: localPdf });

  // PDF → JPG thumbnail (Puppeteer)
  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();

  await page.goto(`file://${localPdf}`, { waitUntil: "networkidle0" });
  await page.setViewport({ width: 1024, height: 1400 });
  await page.screenshot({
    path: localJpg,
    type: "jpeg",
    quality: 80,
  });

  await browser.close();

  // GCS へアップロード
  const thumbFile = bucket.file(`fax_thumbs/${messageId}.jpg`);
  await thumbFile.save(await fs.readFile(localJpg), {
    resumable: false,
    contentType: "image/jpeg",
  });

  return `gs://${bucket.name}/fax_thumbs/${messageId}.jpg`;
}
