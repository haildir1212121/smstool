const { app } = require("@azure/functions");
const { getCosmosClient } = require("../shared/cosmos");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// GET /api/logs - Get activity logs
app.http("getLogs", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "logs",
  handler: requireAuth(async (request, context) => {
    const limitParam = request.query.get("limit");
    const maxItems = limitParam ? Math.min(parseInt(limitParam, 10), 500) : 200;

    const { logsContainer } = getCosmosClient();

    const { resources } = await logsContainer.items
      .query({
        query: "SELECT * FROM c WHERE c.orgId = @orgId ORDER BY c.createdAtMs DESC OFFSET 0 LIMIT @limit",
        parameters: [
          { name: "@orgId", value: ORG_ID },
          { name: "@limit", value: maxItems },
        ],
      })
      .fetchAll();

    return { jsonBody: { success: true, logs: resources } };
  }),
});

// POST /api/logs - Create a new log entry
app.http("createLog", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "logs",
  handler: requireAuth(async (request, context) => {
    const body = await request.json();
    const { logsContainer } = getCosmosClient();

    const logId = `log-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const logData = {
      id: logId,
      orgId: ORG_ID,
      type: body.type || "general",
      threadName: body.threadName || "",
      phone: body.phone || "",
      message: body.message || "",
      threadId: body.threadId || "",
      count: body.count || 0,
      createdAtMs: Date.now(),
    };

    const { resource } = await logsContainer.items.upsert(logData);
    return { jsonBody: { success: true, log: resource } };
  }),
});
