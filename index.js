// index.js
import express from "express";

const app = express();
app.use(express.json());

// Gmail Pub/Sub push endpoint
app.post("/gmail/push", (req, res) => {
  console.log("📩 Gmail Push Notification:", JSON.stringify(req.body, null, 2));
  res.status(200).send("OK"); // Gmail API は 200 が返ってくればOK
});

// ヘルスチェック用
app.get("/", (req, res) => {
  res.send("Cloud Run is running!");
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
