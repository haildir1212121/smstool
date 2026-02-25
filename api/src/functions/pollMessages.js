const { app } = require("@azure/functions");
const { getCosmosClient } = require("../shared/cosmos");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// GET /api/poll-messages?after=<timestamp> - Poll for new inbound messages
// This replaces the Twilio get-messages endpoint.
// In production, configure an Azure Communication Services Event Grid webhook
// that writes inbound messages to Cosmos DB via a separate Azure Function.
// This endpoint then serves as the polling layer for the frontend.
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

// POST /api/inbound-sms - Webhook for Azure Communication Services Event Grid
// This receives inbound SMS events and stores them in Cosmos DB.
// Configure Event Grid subscription: ACS SMS Received → this endpoint.
app.http("inboundSms", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "inbound-sms",
  handler: async (request, context) => {
    const body = await request.json();

    // Handle Event Grid validation handshake
    if (Array.isArray(body) && body[0]?.eventType === "Microsoft.EventGrid.SubscriptionValidationEvent") {
      return {
        jsonBody: {
          validationResponse: body[0].data.validationCode,
        },
      };
    }

    const { threadsContainer } = getCosmosClient();

    const events = Array.isArray(body) ? body : [body];

    for (const event of events) {
      if (event.eventType === "Microsoft.Communication.SMSReceived") {
        const data = event.data;
        const phone = data.from;
        const messageBody = data.message;
        const receivedAt = new Date(data.receivedTimestamp).getTime();
        const messageId = event.id || `inbound-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

        // Store the message
        await threadsContainer.items.upsert({
          id: messageId,
          orgId: ORG_ID,
          threadId: phone,
          type: "message",
          body: messageBody,
          direction: "received",
          createdAtMs: receivedAt,
          sid: messageId,
          mediaUrls: [],
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
              phone: phone,
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
      }
    }

    return { status: 200, jsonBody: { success: true } };
  },
});
