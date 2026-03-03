const { app } = require("@azure/functions");
const { BlobServiceClient } = require("@azure/storage-blob");
const { requireAuth } = require("../shared/auth");

const ORG_ID = "dispatch_team_main";

// POST /api/upload-media - Upload media files to Azure Blob Storage
app.http("uploadMedia", {
  methods: ["POST"],
  authLevel: "anonymous",
  route: "upload-media",
  handler: requireAuth(async (request, context) => {
    const threadId = request.query.get("threadId");
    if (!threadId) {
      return { status: 400, jsonBody: { error: "Missing threadId query parameter" } };
    }

    const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
    const containerName = process.env.AZURE_STORAGE_CONTAINER || "media";

    if (!connectionString) {
      return { status: 500, jsonBody: { error: "Storage not configured" } };
    }

    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerClient = blobServiceClient.getContainerClient(containerName);
    await containerClient.createIfNotExists({ access: "blob" });

    const formData = await request.formData();
    const uploadedUrls = [];

    let index = 0;
    for (const [key, value] of formData.entries()) {
      if (value instanceof Blob) {
        const timestamp = Date.now();
        const blobName = `mms/${ORG_ID}/${threadId}/${timestamp}_${index}`;
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);

        const buffer = Buffer.from(await value.arrayBuffer());
        await blockBlobClient.upload(buffer, buffer.length, {
          blobHTTPHeaders: { blobContentType: value.type },
        });

        uploadedUrls.push(blockBlobClient.url);
        index++;
      }
    }

    return { jsonBody: { success: true, urls: uploadedUrls } };
  }),
});

// GET /api/upload-sas - Get a SAS URL for direct browser upload (alternative approach)
app.http("getUploadSas", {
  methods: ["GET"],
  authLevel: "anonymous",
  route: "upload-sas",
  handler: requireAuth(async (request, context) => {
    const threadId = request.query.get("threadId");
    const fileName = request.query.get("fileName");

    if (!threadId || !fileName) {
      return { status: 400, jsonBody: { error: "Missing threadId or fileName" } };
    }

    const { BlobSASPermissions, generateBlobSASQueryParameters, StorageSharedKeyCredential } =
      require("@azure/storage-blob");

    const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
    const containerName = process.env.AZURE_STORAGE_CONTAINER || "media";
    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerClient = blobServiceClient.getContainerClient(containerName);

    const blobName = `mms/${ORG_ID}/${threadId}/${Date.now()}_${fileName}`;
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    // For SAS generation, extract account name and key from connection string
    const accountName = connectionString.match(/AccountName=([^;]+)/)?.[1];
    const accountKey = connectionString.match(/AccountKey=([^;]+)/)?.[1];

    if (!accountName || !accountKey) {
      return { status: 500, jsonBody: { error: "Storage credentials not available for SAS" } };
    }

    const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
    const expiresOn = new Date(Date.now() + 30 * 60 * 1000); // 30 minutes

    const sasToken = generateBlobSASQueryParameters(
      {
        containerName,
        blobName,
        permissions: BlobSASPermissions.parse("cw"),
        expiresOn,
      },
      sharedKeyCredential
    ).toString();

    return {
      jsonBody: {
        success: true,
        uploadUrl: `${blockBlobClient.url}?${sasToken}`,
        blobUrl: blockBlobClient.url,
      },
    };
  }),
});
