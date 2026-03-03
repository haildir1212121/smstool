#!/usr/bin/env node

/**
 * Firebase Firestore → Azure Cosmos DB Migration Script
 *
 * Migrates all data from the existing Firebase Firestore database
 * (project: sms-dlx) to Azure Cosmos DB.
 *
 * Prerequisites:
 *   npm install firebase-admin @azure/cosmos @azure/storage-blob
 *
 * Required environment variables:
 *   FIREBASE_SERVICE_ACCOUNT_PATH - Path to Firebase service account JSON key file
 *   COSMOS_ENDPOINT               - Azure Cosmos DB endpoint URL
 *   COSMOS_KEY                     - Azure Cosmos DB primary key
 *   COSMOS_DATABASE                - Cosmos DB database name (default: dispatchcommand)
 *
 * Usage:
 *   export FIREBASE_SERVICE_ACCOUNT_PATH="./firebase-sa-key.json"
 *   export COSMOS_ENDPOINT="https://your-account.documents.azure.com:443/"
 *   export COSMOS_KEY="your-key"
 *   node scripts/migrate-firebase-to-cosmos.js
 *
 * The script is idempotent — re-running it will upsert existing records.
 */

const admin = require("firebase-admin");
const { CosmosClient } = require("@azure/cosmos");

const ORG_ID = process.env.ORG_ID || "dispatch_team_main";
const DISCOVER_MODE = process.argv.includes("--discover");

async function main() {
  // ── Validate env ──────────────────────────────────────────────────────
  const saPath = process.env.FIREBASE_SERVICE_ACCOUNT_PATH;
  const cosmosEndpoint = process.env.COSMOS_ENDPOINT;
  const cosmosKey = process.env.COSMOS_KEY;
  const cosmosDbName = process.env.COSMOS_DATABASE || "dispatchcommand";

  if (!saPath) {
    console.error("Missing FIREBASE_SERVICE_ACCOUNT_PATH");
    process.exit(1);
  }

  // ── Initialize Firebase Admin ─────────────────────────────────────────
  const serviceAccount = require(saPath.startsWith("/") ? saPath : `${process.cwd()}/${saPath}`);
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
  const firestore = admin.firestore();

  // ── Discovery mode: list top-level collections and structure ─────────
  if (DISCOVER_MODE) {
    console.log("══════════════════════════════════════════════════════════");
    console.log("  DISCOVERY MODE — scanning Firestore structure");
    console.log("══════════════════════════════════════════════════════════\n");

    const topCollections = await firestore.listCollections();
    if (topCollections.length === 0) {
      console.log("  No top-level collections found. Is the service account for the right project?");
      process.exit(0);
    }

    for (const col of topCollections) {
      console.log(`Collection: ${col.id}`);
      const docs = await col.limit(5).get();
      for (const doc of docs.docs) {
        console.log(`  Doc: ${col.id}/${doc.id}`);
        const subCollections = await doc.ref.listCollections();
        for (const sub of subCollections) {
          const subDocs = await sub.limit(3).get();
          console.log(`    Sub-collection: ${sub.id} (${subDocs.size}+ docs)`);
          if (subDocs.size > 0) {
            const firstDoc = subDocs.docs[0];
            const subSubCollections = await firstDoc.ref.listCollections();
            for (const subSub of subSubCollections) {
              const subSubDocs = await subSub.limit(1).get();
              console.log(`      Sub-sub-collection: ${subSub.id} (${subSubDocs.size}+ docs)`);
            }
          }
        }
      }
    }

    console.log("\n══════════════════════════════════════════════════════════");
    console.log("  Once you identify the right paths, run the migration:");
    console.log("  Set ORG_ID env var if the org doc is not 'dispatch_team_main'");
    console.log("  Example: ORG_ID=my_org_id node migrate-firebase-to-cosmos.js");
    console.log("══════════════════════════════════════════════════════════");
    process.exit(0);
  }

  // ── Full migration: validate Cosmos env vars ──────────────────────────
  if (!cosmosEndpoint || !cosmosKey) {
    console.error("Missing required environment variables.");
    console.error("  COSMOS_ENDPOINT, COSMOS_KEY");
    process.exit(1);
  }

  // ── Initialize Cosmos DB ──────────────────────────────────────────────
  const cosmosClient = new CosmosClient({ endpoint: cosmosEndpoint, key: cosmosKey });

  console.log("Creating Cosmos DB database and containers...");
  await cosmosClient.databases.createIfNotExists({ id: cosmosDbName });
  const database = cosmosClient.database(cosmosDbName);

  await database.containers.createIfNotExists({
    id: "threads",
    partitionKey: { paths: ["/orgId"] },
  });
  await database.containers.createIfNotExists({
    id: "logs",
    partitionKey: { paths: ["/orgId"] },
  });

  const threadsContainer = database.container("threads");
  const logsContainer = database.container("logs");

  // ── Migrate Threads ───────────────────────────────────────────────────
  console.log(`\nUsing ORG_ID: "${ORG_ID}"`);
  console.log("\n── Migrating Threads ──────────────────────────────────────");
  const threadsSnap = await firestore
    .collection("organizations")
    .doc(ORG_ID)
    .collection("threads")
    .get();

  let threadCount = 0;
  let messageCount = 0;

  for (const threadDoc of threadsSnap.docs) {
    const data = threadDoc.data();
    const threadId = threadDoc.id;

    // Convert Firestore Timestamps to milliseconds
    const threadItem = {
      id: threadId,
      orgId: ORG_ID,
      type: "thread",
      phone: data.phone || threadId,
      name: data.name || threadId,
      unread: data.unread || 0,
      lastMessageText: data.lastMessageText || "",
      lastMessageAtMs: toMs(data.lastMessageAtMs) || toMs(data.createdAt) || Date.now(),
      lastReadAtMs: toMs(data.lastReadAtMs) || 0,
      isUrgent: data.isUrgent || false,
      tags: data.tags || [],
      notes: data.notes || "",
      createdAt: toMs(data.createdAt) || Date.now(),
      updatedAt: Date.now(),
    };

    await threadsContainer.items.upsert(threadItem);
    threadCount++;
    process.stdout.write(`  Thread ${threadCount}: ${threadItem.name} (${threadId})\n`);

    // ── Migrate Messages for this thread ──────────────────────────────
    const msgsSnap = await firestore
      .collection("organizations")
      .doc(ORG_ID)
      .collection("threads")
      .doc(threadId)
      .collection("messages")
      .orderBy("createdAt", "asc")
      .get();

    for (const msgDoc of msgsSnap.docs) {
      const msg = msgDoc.data();
      const messageItem = {
        id: msgDoc.id,
        orgId: ORG_ID,
        threadId: threadId,
        type: "message",
        body: msg.body || "",
        direction: msg.direction || "received",
        createdAtMs: toMs(msg.createdAt) || Date.now(),
        sid: msg.sid || msgDoc.id,
        mediaUrls: msg.mediaUrls || [],
      };

      await threadsContainer.items.upsert(messageItem);
      messageCount++;
    }

    process.stdout.write(`    → ${msgsSnap.size} messages migrated\n`);
  }

  // ── Migrate Logs ──────────────────────────────────────────────────────
  console.log("\n── Migrating Logs ────────────────────────────────────────");
  const logsSnap = await firestore
    .collection("organizations")
    .doc(ORG_ID)
    .collection("logs")
    .get();

  let logCount = 0;
  for (const logDoc of logsSnap.docs) {
    const data = logDoc.data();
    const logItem = {
      id: logDoc.id,
      orgId: ORG_ID,
      type: data.type || "general",
      threadName: data.threadName || "",
      phone: data.phone || "",
      message: data.message || "",
      threadId: data.threadId || "",
      count: data.count || 0,
      createdAtMs: toMs(data.createdAt) || Date.now(),
    };

    await logsContainer.items.upsert(logItem);
    logCount++;
  }

  // ── Summary ───────────────────────────────────────────────────────────
  console.log("\n══════════════════════════════════════════════════════════");
  console.log("  Migration Complete!");
  console.log(`  Threads:  ${threadCount}`);
  console.log(`  Messages: ${messageCount}`);
  console.log(`  Logs:     ${logCount}`);
  console.log("══════════════════════════════════════════════════════════\n");

  process.exit(0);
}

/** Convert Firestore Timestamp or number to milliseconds. */
function toMs(val) {
  if (!val) return null;
  if (typeof val === "number") return val;
  if (typeof val.toMillis === "function") return val.toMillis();
  if (val._seconds != null) return val._seconds * 1000 + Math.floor((val._nanoseconds || 0) / 1e6);
  if (val instanceof Date) return val.getTime();
  return null;
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
