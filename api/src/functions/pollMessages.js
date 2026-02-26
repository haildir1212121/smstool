const { app } = require("@azure/functions");
const twilio = require("twilio");
const { getCosmosClient } = require("../shared/cosmos");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// GET /api/poll-messages?after=<timestamp> - Poll Cosmos DB for new inbound messages
app.http("pollMessages", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "poll-messages",
  handler: requireAuth(async (request, context) => {
    const afterParam = request.query.get("after");
    const after = afterParam ? parseInt(afterParam, 10) : Date.now() - 15000;

    const { threadsContainer } = getCosmosClient();

    const { resources } = await threadsContainer.items
      .query({
        query:
          "SELECT * FROM c WHERE c.orgId = @orgId AND c.type = 'message' AND c.direction = 'received' AND c.createdAtMs > @after ORDER BY c.createdAtMs ASC",
        parameters: [
          { name: "@orgId", value: ORG_ID },
          { name: "@after", value: after },
        ],
      })
      .fetchAll();

    const messages = resources.map((m) => ({
      from: m.threadId,
      body: m.body,
      dateCreated: new Date(m.createdAtMs).toISOString(),
      sid: m.sid,
      direction: m.direction,
      mediaUrls: m.mediaUrls || [],
    }));

    return { jsonBody: { success: true, messages } };
  }),
});

// POST /api/inbound-sms - Twilio webhook for incoming SMS/MMS
// Configure this URL in Twilio Console → Phone Number → Messaging → "A message comes in"
// Twilio sends form-encoded POST data; this function validates the signature and stores the message.
app.http("inboundSms", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "inbound-sms",
  handler: async (request, context) => {
    // Parse the form-encoded body Twilio sends
    const formData = await request.formData();
    const params = {};
    for (const [key, value] of formData.entries()) {
      params[key] = value;
    }

    // Validate Twilio request signature (skip if auth token not set, e.g. local dev)
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    if (authToken) {
      const twilioSignature = request.headers.get("x-twilio-signature") || "";
      const webhookUrl = process.env.TWILIO_WEBHOOK_URL || `${request.url.split("?")[0]}`;
      const isValid = twilio.validateRequest(authToken, twilioSignature, webhookUrl, params);
      if (!isValid) {
        context.log("Invalid Twilio signature");
        return { status: 403, jsonBody: { error: "Invalid signature" } };
      }
    }

    const phone = params.From;
    const messageBody = params.Body || "";
    const messageSid = params.MessageSid;
    const numMedia = parseInt(params.NumMedia || "0", 10);
    const receivedAt = Date.now();

    // Collect media URLs (MMS attachments)
    const mediaUrls = [];
    for (let i = 0; i < numMedia; i++) {
      const url = params[`MediaUrl${i}`];
      if (url) mediaUrls.push(url);
    }

    if (!phone) {
      return { status: 400, body: "<Response></Response>", headers: { "Content-Type": "text/xml" } };
    }

    const { threadsContainer } = getCosmosClient();

    // Store the inbound message
    await threadsContainer.items.upsert({
      id: messageSid || `inbound-${receivedAt}-${Math.random().toString(36).slice(2, 8)}`,
      orgId: ORG_ID,
      threadId: phone,
      type: "message",
      body: messageBody,
      direction: "received",
      createdAtMs: receivedAt,
      sid: messageSid,
      mediaUrls,
    });

    // Update or create thread
    try {
      const { resource: existing } = await threadsContainer.item(phone, ORG_ID).read();
      await threadsContainer.items.upsert({
        ...existing,
        lastMessageText: messageBody,
        lastMessageAtMs: receivedAt,
        unread: (existing.unread || 0) + 1,
        updatedAt: Date.now(),
      });
    } catch (e) {
      if (e.code === 404) {
        await threadsContainer.items.upsert({
          id: phone,
          orgId: ORG_ID,
          type: "thread",
          phone,
          name: phone,
          unread: 1,
          lastMessageText: messageBody,
          lastMessageAtMs: receivedAt,
          isUrgent: false,
          tags: [],
          notes: "",
          createdAt: Date.now(),
          updatedAt: Date.now(),
        });
      }
    }

    // Respond with empty TwiML so Twilio knows we received it
    return {
      status: 200,
      body: "<Response></Response>",
      headers: { "Content-Type": "text/xml" },
    };
  },
});
