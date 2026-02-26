const { app } = require("@azure/functions");
const twilio = require("twilio");
const { requireAuth } = require("../shared/auth");

// POST /api/send-sms - Send an SMS/MMS via Twilio
app.http("sendSms", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "send-sms",
  handler: requireAuth(async (request, context) => {
    const body = await request.json();
    const { to, body: messageBody, mediaUrls } = body;

    if (!to || !messageBody) {
      return {
        status: 400,
        jsonBody: { error: "Missing required fields: to, body" },
      };
    }

    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const fromNumber = process.env.TWILIO_PHONE_NUMBER;

    if (!accountSid || !authToken || !fromNumber) {
      return {
        status: 500,
        jsonBody: { error: "SMS service not configured" },
      };
    }

    try {
      const client = twilio(accountSid, authToken);

      const messageOptions = {
        from: fromNumber,
        to,
        body: messageBody,
      };

      // Attach media URLs for MMS if provided
      if (mediaUrls && mediaUrls.length > 0) {
        messageOptions.mediaUrl = mediaUrls;
      }

      const message = await client.messages.create(messageOptions);

      return {
        jsonBody: {
          success: true,
          messageId: message.sid,
        },
      };
    } catch (e) {
      context.log("SMS send error:", e.message);
      return {
        status: 500,
        jsonBody: { error: "Failed to send SMS" },
      };
    }
  }),
});
