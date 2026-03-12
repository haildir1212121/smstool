/**
 * Twilio Function: Inbound SMS Webhook → Firestore
 *
 * Deploy this as a Twilio Function (https://www.twilio.com/docs/serverless/functions-assets/functions).
 *
 * SETUP:
 *   1. Go to Twilio Console → Functions & Assets → Services
 *   2. Create a new service (or use your existing "sms-backend-3488" service)
 *   3. Add this as a new function at path: /incoming-sms
 *   4. Add dependency: firebase-admin (latest)
 *   5. Add environment variables:
 *        FIREBASE_PROJECT_ID    = sms-dlx
 *        FIREBASE_CLIENT_EMAIL  = <your service account email>
 *        FIREBASE_PRIVATE_KEY   = <your service account private key>
 *   6. Deploy the service
 *   7. In Twilio Console → Phone Numbers → your number → Messaging Configuration:
 *        Set "A message comes in" webhook to:
 *        https://sms-backend-3488.twil.io/incoming-sms
 *
 * This replaces the browser-side polling loop. Twilio POSTs here on every
 * inbound SMS, this function writes directly to Firestore, and the existing
 * onSnapshot listeners in index.html pick up the change instantly.
 */

const admin = require("firebase-admin");

let firestore;

function getFirestore(context) {
  if (!firestore) {
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId: context.FIREBASE_PROJECT_ID,
        clientEmail: context.FIREBASE_CLIENT_EMAIL,
        privateKey: (context.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
      }),
    });
    firestore = admin.firestore();
  }
  return firestore;
}

const SHARED_ORG_ID = "dispatch_team_main";

const VEHICLE_KEYWORDS = [
  "oil change", "repair", "red triangle", "check engine", "engine light",
  "won't start", "wont start", "not starting", "flat tire",
  "break down", "breakdown", "tow", "not turning on",
];

function normalizePhone(p) {
  if (!p) return "";
  const clean = p.replace(/\D/g, "");
  if (clean.length === 10) return `+1${clean}`;
  if (clean.length === 11 && clean.startsWith("1")) return `+${clean}`;
  return clean.length > 0 ? `+${clean}` : "";
}

exports.handler = async function (context, event, callback) {
  const twiml = new Twilio.twiml.MessagingResponse();

  try {
    const db = getFirestore(context);

    const from = normalizePhone(event.From);
    const body = event.Body || "";
    const sid = event.MessageSid;
    const numMedia = parseInt(event.NumMedia || "0", 10);
    const msgTime = Date.now();

    // Collect media URLs
    const mediaUrls = [];
    for (let i = 0; i < numMedia; i++) {
      const url = event[`MediaUrl${i}`];
      if (url) mediaUrls.push(url);
    }

    const threadRef = db.doc(`organizations/${SHARED_ORG_ID}/threads/${from}`);
    const msgRef = db.doc(`organizations/${SHARED_ORG_ID}/threads/${from}/messages/${sid}`);

    // Write message document
    await msgRef.set({
      body,
      direction: "received",
      createdAt: admin.firestore.Timestamp.fromMillis(msgTime),
      sid,
      mediaUrls,
    });

    // Check vehicle keywords
    const lowerBody = body.toLowerCase();
    const isMaintenance = VEHICLE_KEYWORDS.some((k) => lowerBody.includes(k));

    // Get existing thread to determine name and check timing
    const threadSnap = await threadRef.get();
    const threadData = threadSnap.exists ? threadSnap.data() : null;
    const threadName = threadData ? (threadData.name || from) : from;
    const currentLastMs = threadData ? (threadData.lastMessageAtMs || 0) : 0;

    // Update thread metadata
    const updateData = {
      id: from,
      phone: from,
      lastMessageText: body,
      lastMessageAtMs: msgTime,
      unread: admin.firestore.FieldValue.increment(1),
    };

    if (isMaintenance) updateData.isUrgent = true;
    if (!threadData) updateData.name = from;

    if (msgTime > currentLastMs) {
      await threadRef.set(updateData, { merge: true });
    }

    // Log vehicle alert if applicable
    if (isMaintenance) {
      await db.collection(`organizations/${SHARED_ORG_ID}/logs`).add({
        type: "vehicle_alert",
        threadName,
        phone: from,
        message: body,
        threadId: from,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  } catch (err) {
    console.error("Webhook error:", err);
  }

  // Always return 200 with empty TwiML so Twilio doesn't retry
  callback(null, twiml);
};
