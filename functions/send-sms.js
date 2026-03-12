/**
 * Twilio Function: Send SMS
 *
 * Deploy at path: /send-sms
 *
 * No extra env vars needed — Twilio Functions automatically have access
 * to your Account SID and Auth Token via context.getTwilioClient().
 *
 * Required env variable:
 *   TWILIO_PHONE_NUMBER = your Twilio phone number (e.g. +1234567890)
 */

exports.handler = async function (context, event, callback) {
  // CORS headers so the browser can call this from any origin
  const response = new Twilio.Response();
  response.appendHeader("Access-Control-Allow-Origin", "*");
  response.appendHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  response.appendHeader("Access-Control-Allow-Headers", "Content-Type");

  // Handle preflight OPTIONS request
  if (event.request && event.request.method === "OPTIONS") {
    response.setStatusCode(200);
    return callback(null, response);
  }

  try {
    const client = context.getTwilioClient();
    const { to, body, mediaUrls } = event;

    if (!to || !body) {
      response.setStatusCode(400);
      response.setBody({ success: false, error: "Missing 'to' or 'body'" });
      return callback(null, response);
    }

    const msgParams = {
      to,
      from: context.TWILIO_PHONE_NUMBER,
      body,
    };

    if (mediaUrls && mediaUrls.length > 0) {
      msgParams.mediaUrl = mediaUrls;
    }

    const message = await client.messages.create(msgParams);

    response.setStatusCode(200);
    response.setBody({ success: true, sid: message.sid });
    callback(null, response);
  } catch (err) {
    console.error("send-sms error:", err);
    response.setStatusCode(500);
    response.setBody({ success: false, error: err.message });
    callback(null, response);
  }
};
