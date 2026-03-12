require("dotenv").config();
const express = require("express");
const cors = require("cors");
const twilio = require("twilio");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Serve static files (index.html, etc.)
app.use(express.static(path.join(__dirname)));

const client = twilio(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN
);

// Send SMS endpoint
app.post("/send-sms", async (req, res) => {
  try {
    const { to, body, mediaUrls } = req.body;
    if (!to || !body) {
      return res.status(400).json({ success: false, error: "Missing 'to' or 'body'" });
    }

    const msgParams = {
      to,
      from: process.env.TWILIO_PHONE_NUMBER,
      body,
    };

    if (mediaUrls && mediaUrls.length > 0) {
      msgParams.mediaUrl = mediaUrls;
    }

    const message = await client.messages.create(msgParams);
    res.json({ success: true, sid: message.sid });
  } catch (err) {
    console.error("send-sms error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 2000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
