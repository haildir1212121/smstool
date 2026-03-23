/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook        — receives iCabbi "Pre-booking: Driver Designated" events
 * GET  /trips          — returns stored trips for a date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
 * GET  /trips/stats    — quick count of stored trips for a date
 *
 * KV key format:  trips:YYYY-MM-DD  →  JSON array of trip objects
 * TTL: 7 days (604800 seconds)
 */

const SEVEN_DAYS = 604800;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Webhook-Secret",
};

export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    try {
      if (url.pathname === "/webhook" && request.method === "POST") {
        return await handleWebhook(request, env);
      }
      if (url.pathname === "/trips" && request.method === "GET") {
        return await handleGetTrips(request, env);
      }
      if (url.pathname === "/trips/stats" && request.method === "GET") {
        return await handleTripStats(request, env);
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (e) {
      console.error("Worker error:", e);
      return jsonResponse({ error: e.message }, 500);
    }
  },
};

// ── Webhook Handler ──────────────────────────────────────────────────────────

async function handleWebhook(request, env) {
  // Verify shared secret
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();

  // iCabbi may send a single booking or an array
  const bookings = Array.isArray(payload) ? payload : [payload];

  let stored = 0;
  for (const booking of bookings) {
    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    const key = `trips:${trip.date}`;

    // Read existing trips for this date
    const existing = await getTripsFromKV(env, key);

    // Upsert: replace if same trip ID exists, otherwise append
    const idx = existing.findIndex((t) => t.id && t.id === trip.id);
    if (idx >= 0) {
      existing[idx] = trip;
    } else {
      existing.push(trip);
    }

    // Write back with 7-day TTL
    await env.TRIPS.put(key, JSON.stringify(existing), {
      expirationTtl: SEVEN_DAYS,
    });
    stored++;
  }

  return jsonResponse({ ok: true, stored });
}

// ── Trips API ────────────────────────────────────────────────────────────────

async function handleGetTrips(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;

  if (!from) {
    return jsonResponse({ error: "Missing 'from' query parameter (YYYY-MM-DD)" }, 400);
  }

  // Collect trips for every date in range
  const allTrips = {};
  const dates = getDateRange(from, to);

  for (const date of dates) {
    const key = `trips:${date}`;
    const trips = await getTripsFromKV(env, key);
    if (trips.length > 0) {
      allTrips[date] = trips;
    }
  }

  return jsonResponse({ from, to, dates: dates.length, trips: allTrips });
}

async function handleTripStats(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;

  if (!from) {
    return jsonResponse({ error: "Missing 'from' query parameter" }, 400);
  }

  const dates = getDateRange(from, to);
  const stats = {};
  let total = 0;

  for (const date of dates) {
    const trips = await getTripsFromKV(env, `trips:${date}`);
    stats[date] = trips.length;
    total += trips.length;
  }

  return jsonResponse({ from, to, total, byDate: stats });
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function normalizeTrip(raw) {
  // Handle various iCabbi payload shapes
  const booking = raw.booking || raw.data || raw;

  const id = String(
    booking.perma_id || booking.id || booking.booking_id || ""
  ).trim();

  // Extract date from various fields
  const rawDateTime =
    booking.scheduled_pickup_date ||
    booking.pickup_date ||
    booking.date ||
    booking.release_date ||
    "";

  let date = "";
  let time = "";
  if (rawDateTime) {
    try {
      const dt = new Date(rawDateTime);
      date = dt.toISOString().split("T")[0];
      time = rawDateTime; // preserve full timestamp for sorting
    } catch (e) {
      // Try to extract date directly
      const match = rawDateTime.match(/(\d{4}-\d{2}-\d{2})/);
      if (match) date = match[1];
    }
  }

  // Also check for separate time field
  if (!time) {
    time =
      booking.scheduled_pickup_time ||
      booking.pickup_time ||
      booking.time ||
      "";
  }

  const vehicleRef = String(
    booking.driver?.vehicle?.ref ||
      booking.vehicle_ref ||
      booking.vehicleRef ||
      booking.vehicle_id ||
      ""
  ).trim();

  return {
    id,
    date,
    time,
    vehicleRef,
    pickup:
      booking.address_formatted ||
      booking.pickup_address ||
      booking.from ||
      "",
    dropoff:
      booking.dropoff_address_formatted ||
      booking.dropoff_address ||
      booking.to ||
      "",
    passenger:
      booking.name ||
      booking.passenger_name ||
      booking.customer_name ||
      "",
    passengerPhone: booking.phone || booking.passenger_phone || "",
    notes: booking.notes || booking.driver_notes || "",
    status: booking.status || "",
    receivedAt: new Date().toISOString(),
  };
}

async function getTripsFromKV(env, key) {
  const raw = await env.TRIPS.get(key);
  if (!raw) return [];
  try {
    return JSON.parse(raw);
  } catch {
    return [];
  }
}

function getDateRange(from, to) {
  const dates = [];
  const start = new Date(from + "T12:00:00Z");
  const end = new Date(to + "T12:00:00Z");

  if (isNaN(start.getTime()) || isNaN(end.getTime())) return [from];

  const current = new Date(start);
  while (current <= end) {
    dates.push(current.toISOString().split("T")[0]);
    current.setDate(current.getDate() + 1);
  }
  return dates;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS,
    },
  });
}
