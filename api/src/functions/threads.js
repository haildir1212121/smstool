const { app } = require("@azure/functions");
const { getCosmosClient } = require("../shared/cosmos");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// GET /api/threads - List all threads for the organization
app.http("getThreads", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "threads",
  handler: requireAuth(async (request, context) => {
    const { threadsContainer } = getCosmosClient();

    const { resources } = await threadsContainer.items
      .query({
        query: "SELECT * FROM c WHERE c.orgId = @orgId ORDER BY c.lastMessageAtMs DESC",
        parameters: [{ name: "@orgId", value: ORG_ID }],
      })
      .fetchAll();

    return { jsonBody: { success: true, threads: resources } };
  }),
});

// GET /api/threads/:id - Get a single thread
app.http("getThread", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "threads/{id}",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.id;
    const { threadsContainer } = getCosmosClient();

    try {
      const { resource } = await threadsContainer.item(threadId, ORG_ID).read();
      if (!resource) {
        return { status: 404, jsonBody: { error: "Thread not found" } };
      }
      return { jsonBody: { success: true, thread: resource } };
    } catch (e) {
      if (e.code === 404) {
        return { status: 404, jsonBody: { error: "Thread not found" } };
      }
      throw e;
    }
  }),
});

// PUT /api/threads/:id - Create or update a thread
app.http("upsertThread", {
  methods: ["PUT"],
  authLevel: "anonymous",
  route: "threads/{id}",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.id;
    const body = await request.json();
    const { threadsContainer } = getCosmosClient();

    const threadData = {
      ...body,
      id: threadId,
      orgId: ORG_ID,
      updatedAt: Date.now(),
    };

    const { resource } = await threadsContainer.items.upsert(threadData);
    return { jsonBody: { success: true, thread: resource } };
  }),
});

// PATCH /api/threads/:id - Partial update a thread
app.http("patchThread", {
  methods: ["PATCH"],
  authLevel: "anonymous",
  route: "threads/{id}",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.id;
    const updates = await request.json();
    const { threadsContainer } = getCosmosClient();

    try {
      const { resource: existing } = await threadsContainer.item(threadId, ORG_ID).read();
      if (!existing) {
        return { status: 404, jsonBody: { error: "Thread not found" } };
      }

      // Handle increment for unread count
      if (updates.unreadIncrement) {
        existing.unread = (existing.unread || 0) + updates.unreadIncrement;
        delete updates.unreadIncrement;
      }

      const merged = { ...existing, ...updates, updatedAt: Date.now() };
      const { resource } = await threadsContainer.items.upsert(merged);
      return { jsonBody: { success: true, thread: resource } };
    } catch (e) {
      if (e.code === 404) {
        // Thread doesn't exist yet, create it
        const threadData = {
          ...updates,
          id: threadId,
          orgId: ORG_ID,
          updatedAt: Date.now(),
        };
        const { resource } = await threadsContainer.items.upsert(threadData);
        return { jsonBody: { success: true, thread: resource } };
      }
      throw e;
    }
  }),
});

// DELETE /api/threads/:id - Delete a thread and its messages
app.http("deleteThread", {
  methods: ["DELETE"],
  authLevel: "anonymous",
  route: "threads/{id}",
  handler: requireAuth(async (request, context) => {
    const threadId = request.params.id;
    const { threadsContainer } = getCosmosClient();

    try {
      await threadsContainer.item(threadId, ORG_ID).delete();
    } catch (e) {
      if (e.code !== 404) throw e;
    }

    return { jsonBody: { success: true } };
  }),
});
