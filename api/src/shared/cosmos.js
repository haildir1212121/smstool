const { CosmosClient } = require("@azure/cosmos");

let client;
let database;
let threadsContainer;
let logsContainer;

function getCosmosClient() {
  if (!client) {
    client = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key: process.env.COSMOS_KEY,
    });
    database = client.database(process.env.COSMOS_DATABASE || "dispatchcommand");
    threadsContainer = database.container("threads");
    logsContainer = database.container("logs");
  }
  return { client, database, threadsContainer, logsContainer };
}

async function ensureDatabase() {
  const { client } = getCosmosClient();
  const dbName = process.env.COSMOS_DATABASE || "dispatchcommand";

  await client.databases.createIfNotExists({ id: dbName });
  const db = client.database(dbName);

  await db.containers.createIfNotExists({
    id: "threads",
    partitionKey: { paths: ["/orgId"] },
  });

  await db.containers.createIfNotExists({
    id: "logs",
    partitionKey: { paths: ["/orgId"] },
  });

  return db;
}

module.exports = { getCosmosClient, ensureDatabase };
