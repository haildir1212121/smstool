/**
 * iCabbi Trip Webhook Worker
 *
 * Receives "Pre-booking: Driver Designated" webhooks from iCabbi,
 * stores trip data in KV keyed by date, and serves it to the frontend.
 *
 * KV structure:
 *   Key: "trips:YYYY-MM-DD"  →  Value: JSON array of trip objects
 *   TTL: 7 days (auto-cleanup)
 *
 * Routes:
 *   POST /webhook          — iCabbi pushes designated trip data here
 *   GET  /trips?from=&to=  — Frontend fetches trips for date range
 *   GET  /health           — Health check
 */

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Webhook-Secret",
};

const KV_TTL = 7 * 24 * 60 * 60; // 7 days in seconds

export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/webhook" && request.method === "POST") {
        return await handleWebhook(request, env);
      }
      if (path === "/trips" && request.method === "GET") {
        return await handleGetTrips(request, env);
      }
      if (path === "/health") {
        return jsonResponse({ status: "ok", timestamp: new Date().toISOString() });
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (err) {
      console.error("Worker error:", err);
      return jsonResponse({ error: "Internal server error", message: err.message }, 500);
    }
  },
};

/**
 * POST /webhook
 *
 * Expects iCabbi "Pre-booking: Driver Designated" event payload.
 * Authenticates via X-Webhook-Secret header matching WEBHOOK_SECRET env var.
 *
 * iCabbi webhook payloads typically include:
 *   - booking_id / id
 *   - pickup_date / date
 *   - passenger name / phone
 *   - pickup address
 *   - dropoff address
 *   - vehicle ref / driver info
 *   - notes
 */
async function handleWebhook(request, env) {
  // Authenticate
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("Webhook received:", JSON.stringify(payload).slice(0, 1000));

  // Normalize — iCabbi may send a single object or wrapped in an event envelope
  const trip = payload.booking || payload.data || payload;

  // Extract the trip date (YYYY-MM-DD) for KV key
  const rawDate = trip.date || trip.pickup_date || trip.release_date || trip.scheduled_time || "";
  if (!rawDate) {
    return jsonResponse({ error: "No date found in payload" }, 400);
  }

  let tripDate;
  try {
    tripDate = new Date(rawDate).toISOString().split("T")[0];
  } catch {
    return jsonResponse({ error: "Invalid date in payload" }, 400);
  }

  // Build normalized trip record
  const tripRecord = {
    id: String(trip.id || trip.booking_id || trip.perma_id || `wh-${Date.now()}`),
    date: tripDate,
    time: rawDate,
    vehicle_ref: String(trip.vehicle_ref || trip.driver?.vehicle?.ref || trip.vehicleRef || trip.vehicle_id || ""),
    passenger: trip.name || trip.passenger_name || trip.customer_name || "",
    passenger_phone: trip.phone || trip.passenger_phone || "",
    pickup: trip.address_formatted || trip.pickup_address || trip.from || "",
    dropoff: trip.dropoff_address_formatted || trip.dropoff_address || trip.to || "",
    notes: trip.notes || trip.driver_notes || "",
    status: trip.status || "designated",
    received_at: new Date().toISOString(),
  };

  // Read existing trips for this date, upsert by trip ID
  const kvKey = `trips:${tripDate}`;
  let existing = [];
  try {
    const raw = await env.TRIPS.get(kvKey, "json");
    if (Array.isArray(raw)) existing = raw;
  } catch { /* first trip for this date */ }

  // Upsert: replace if same trip ID exists, otherwise append
  const idx = existing.findIndex((t) => t.id === tripRecord.id);
  if (idx >= 0) {
    existing[idx] = tripRecord;
  } else {
    existing.push(tripRecord);
  }

  // Sort by time
  existing.sort((a, b) => (a.time || "").localeCompare(b.time || ""));

  // Write back with TTL
  await env.TRIPS.put(kvKey, JSON.stringify(existing), { expirationTtl: KV_TTL });

  console.log(`Stored trip ${tripRecord.id} for ${tripDate} (${existing.length} total for date)`);

  return jsonResponse({
    success: true,
    trip_id: tripRecord.id,
    date: tripDate,
    total_trips_for_date: existing.length,
  });
}

/**
 * GET /trips?from=YYYY-MM-DD&to=YYYY-MM-DD
 *
 * Returns all stored trips across the date range.
 * Frontend authenticates with the same webhook secret via Authorization header.
 */
async function handleGetTrips(request, env) {
  // Authenticate frontend requests
  const authHeader = request.headers.get("Authorization") || "";
  const token = authHeader.replace(/^Bearer\s+/i, "");
  if (!env.WEBHOOK_SECRET || token !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;

  if (!from) {
    return jsonResponse({ error: "Missing 'from' query parameter (YYYY-MM-DD)" }, 400);
  }

  // Enumerate all dates in range
  const dates = [];
  const current = new Date(from + "T12:00:00Z");
  const end = new Date(to + "T12:00:00Z");
  while (current <= end) {
    dates.push(current.toISOString().split("T")[0]);
    current.setDate(current.getDate() + 1);
  }

  // Fetch all dates in parallel
  const results = await Promise.all(
    dates.map(async (date) => {
      const raw = await env.TRIPS.get(`trips:${date}`, "json");
      return { date, trips: Array.isArray(raw) ? raw : [] };
    })
  );

  // Flatten into a single array with date info preserved on each trip
  const allTrips = [];
  for (const { trips } of results) {
    allTrips.push(...trips);
  }

  return jsonResponse({
    from,
    to,
    dates_queried: dates,
    total_trips: allTrips.length,
    trips: allTrips,
  });
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS,
    },
  });
}
