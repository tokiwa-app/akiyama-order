import express from "express";
import gmailPoll from "./gmailPoll.js";
import gmailProcessBatch from "./gmailProcessBatch.js";

const app = express();
app.use(express.json());

app.get("/", (_req, res) => res.status(200).send("ok"));
app.post("/gmail/poll", gmailPoll);
app.post("/gmail/process-batch", gmailProcessBatch);

const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => {
  console.log(`listening on ${port}`);
});
