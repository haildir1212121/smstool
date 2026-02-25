const { app } = require("@azure/functions");
const { SmsClient } = require("@azure/communication-sms");
const { requireAuth } = require("../shared/auth");

// POST /api/send-sms - Send an SMS via Azure Communication Services
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

    const connectionString = process.env.ACS_CONNECTION_STRING;
    const fromNumber = process.env.ACS_PHONE_NUMBER;

    if (!connectionString || !fromNumber) {
      return {
        status: 500,
        jsonBody: { error: "SMS service not configured" },
      };
    }

    try {
      const smsClient = new SmsClient(connectionString);

      const sendResults = await smsClient.send({
        from: fromNumber,
        to: [to],
        message: messageBody,
      });

      const result = sendResults[0];
      if (result.successful) {
        return {
          jsonBody: {
            success: true,
            messageId: result.messageId,
          },
        };
      } else {
        return {
          status: 500,
          jsonBody: {
            error: `SMS send failed: ${result.errorMessage}`,
          },
        };
      }
    } catch (e) {
      context.log("SMS send error:", e.message);
      return {
        status: 500,
        jsonBody: { error: "Failed to send SMS" },
      };
    }
  }),
});
