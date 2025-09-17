
import express from "express";
const app = express();

app.get("/", (req, res) => {
  res.send("Hello Akiyama Order!");
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Listening on port ${port}`);
});
