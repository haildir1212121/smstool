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
    booking.perma_id || booking.id || booking.booking_id ||
    booking.driver_id || ""
  ).trim();

  // Extract date from various fields (including iCabbi's job_date)
  const rawDate =
    booking.job_date ||
    booking.scheduled_pickup_date ||
    booking.pickup_date ||
    booking.date ||
    booking.release_date ||
    "";

  let date = "";
  if (rawDate) {
    try {
      const dt = new Date(rawDate);
      if (!isNaN(dt.getTime())) {
        date = dt.toISOString().split("T")[0];
      }
    } catch (e) {
      // ignore
    }
    // Try to extract date directly (YYYY-MM-DD or DD/MM/YYYY etc.)
    if (!date) {
      const isoMatch = rawDate.match(/(\d{4}-\d{2}-\d{2})/);
      if (isoMatch) {
        date = isoMatch[1];
      } else {
        // Handle DD/MM/YYYY or DD-MM-YYYY
        const dmyMatch = rawDate.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
        if (dmyMatch) {
          date = `${dmyMatch[3]}-${dmyMatch[2].padStart(2, "0")}-${dmyMatch[1].padStart(2, "0")}`;
        }
      }
    }
  }

  // If still no date, use today
  if (!date) {
    date = new Date().toISOString().split("T")[0];
  }

  // Extract time (including iCabbi's job_time)
  const time =
    booking.job_time ||
    booking.scheduled_pickup_time ||
    booking.pickup_time ||
    booking.time ||
    "";

  const vehicleRef = String(
    booking.vehicle_number ||
    booking.driver?.vehicle?.ref ||
    booking.vehicle_ref ||
    booking.vehicleRef ||
    booking.vehicle_id ||
    ""
  ).trim();

  // Build driver name from first/last if available
  const driverName = [booking.driver_first, booking.driver_last]
    .filter(Boolean)
    .join(" ");

  return {
    id,
    date,
    time,
    vehicleRef,
    pickup:
      booking.city ||
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
      booking.client_name ||
      booking.name ||
      booking.passenger_name ||
      booking.customer_name ||
      "",
    passengerPhone: booking.phone || booking.passenger_phone || "",
    driverName,
    driverPhone: booking.driver_phone || "",
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
