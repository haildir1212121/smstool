const { app } = require("@azure/functions");
const { getCosmosClient } = require("../shared/cosmos");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// GET /api/threads/:threadId/messages - Get messages for a thread
app.http("getMessages", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "threads/{threadId}/messages",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.threadId;
    const { threadsContainer } = getCosmosClient();

    // Messages are stored as sub-items within the threads container
    // using a composite key: threadId_msg_<messageId>
    const { resources } = await threadsContainer.items
      .query({
        query:
          "SELECT * FROM c WHERE c.orgId = @orgId AND c.threadId = @threadId AND c.type = 'message' ORDER BY c.createdAtMs ASC",
        parameters: [
          { name: "@orgId", value: ORG_ID },
          { name: "@threadId", value: threadId },
        ],
      })
      .fetchAll();

    return { jsonBody: { success: true, messages: resources } };
  }),
});

// POST /api/threads/:threadId/messages - Add a message to a thread
app.http("addMessage", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "threads/{threadId}/messages",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.threadId;
    const body = await request.json();
    const { threadsContainer } = getCosmosClient();

    const messageId = body.sid || `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const messageData = {
      id: messageId,
      orgId: ORG_ID,
      threadId: threadId,
      type: "message",
      body: body.body || "",
      direction: body.direction || "sent",
      createdAtMs: body.createdAtMs || Date.now(),
      sid: body.sid || messageId,
      mediaUrls: body.mediaUrls || [],
    };

    const { resource } = await threadsContainer.items.upsert(messageData);
    return { jsonBody: { success: true, message: resource } };
  }),
});

// DELETE /api/threads/:threadId/messages/:messageId - Delete a single message
app.http("deleteMessage", {
  methods: ["DELETE"],
  authLevel: "anonymous",
  route: "threads/{threadId}/messages/{messageId}",
  handler: requireAuth(async (request, context) => {
    const messageId = request.params.messageId;
    const { threadsContainer } = getCosmosClient();

    try {
      await threadsContainer.item(messageId, ORG_ID).delete();
    } catch (e) {
      if (e.code !== 404) throw e;
    }

    return { jsonBody: { success: true } };
  }),
});
