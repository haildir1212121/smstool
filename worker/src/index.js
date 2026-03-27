/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook              — receives iCabbi "Pre-booking: Driver Designated" events
 * POST /webhook/undesignate  — receives iCabbi "Driver Undesignate" events (deletes trip)
 * GET  /trips                — returns stored trips for a date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
 * GET  /trips/stats          — quick count of stored trips for a date
 *
 * Storage: KV (fast cache) + Firestore (source of truth)
 * KV key format:  trips:YYYY-MM-DD  →  JSON array of trip objects
 * KV TTL: 7 days (604800 seconds)
 * Firestore path:  organizations/{orgId}/trips/{docId}
 */

const SEVEN_DAYS = 604800;
const ORG_ID = "dispatch_team_main";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Webhook-Secret",
};

// ── Firestore REST API helpers ───────────────────────────────────────────────

let cachedAuthToken = null;
let tokenExpiresAt = 0;

async function getFirebaseIdToken(env) {
  // Reuse cached token if still valid (with 5 min buffer)
  if (cachedAuthToken && Date.now() < tokenExpiresAt - 300000) {
    return cachedAuthToken;
  }

  const apiKey = env.FIREBASE_API_KEY;
  if (!apiKey) {
    console.warn("FIREBASE_API_KEY not set — Firestore writes disabled");
    return null;
  }

  // Sign in anonymously to get an ID token
  const res = await fetch(
    `https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=${apiKey}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ returnSecureToken: true }),
    }
  );

  if (!res.ok) {
    console.error("Firebase auth failed:", await res.text());
    return null;
  }

  const data = await res.json();
  cachedAuthToken = data.idToken;
  // ID tokens expire in 1 hour
  tokenExpiresAt = Date.now() + 3600000;
  return cachedAuthToken;
}

function tripToFirestoreDoc(trip) {
  // Convert a trip object to Firestore REST API document format
  const fields = {};
  for (const [key, value] of Object.entries(trip)) {
    if (value === null || value === undefined) {
      fields[key] = { nullValue: null };
    } else if (typeof value === "number") {
      fields[key] = { doubleValue: value };
    } else {
      fields[key] = { stringValue: String(value) };
    }
  }
  return { fields };
}

function firestoreDocToTrip(doc) {
  // Convert Firestore REST API document back to a plain trip object
  const trip = {};
  if (!doc.fields) return trip;
  for (const [key, val] of Object.entries(doc.fields)) {
    if (val.stringValue !== undefined) trip[key] = val.stringValue;
    else if (val.doubleValue !== undefined) trip[key] = val.doubleValue;
    else if (val.integerValue !== undefined) trip[key] = Number(val.integerValue);
    else if (val.nullValue !== undefined) trip[key] = "";
    else trip[key] = "";
  }
  return trip;
}

function getTripDocId(trip) {
  // Generate a stable document ID for a trip
  if (trip.id) return trip.id;
  // Fallback: hash from vehicle + time + passenger
  const raw = `${trip.vehicleRef}_${trip.date}_${trip.time}_${trip.passenger}`;
  return raw.replace(/[^a-zA-Z0-9_-]/g, "_");
}

async function firestoreWriteTrip(env, trip) {
  const token = await getFirebaseIdToken(env);
  if (!token) return;

  const projectId = env.FIREBASE_PROJECT_ID || "sms-dlx";
  const docId = getTripDocId(trip);
  const path = `organizations/${ORG_ID}/trips/${docId}`;
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${path}?key=${env.FIREBASE_API_KEY}`;

  const doc = tripToFirestoreDoc({ ...trip, updatedAt: new Date().toISOString() });

  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(doc),
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Firestore write failed for ${docId}:`, errText);
  } else {
    console.log(`Firestore: wrote trip ${docId}`);
  }
}

async function firestoreDeleteTrip(env, trip) {
  const token = await getFirebaseIdToken(env);
  if (!token) return;

  const projectId = env.FIREBASE_PROJECT_ID || "sms-dlx";
  const docId = getTripDocId(trip);
  const path = `organizations/${ORG_ID}/trips/${docId}`;
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${path}?key=${env.FIREBASE_API_KEY}`;

  const res = await fetch(url, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Firestore delete failed for ${docId}:`, errText);
  } else {
    console.log(`Firestore: deleted trip ${docId}`);
  }
}

// ── Main Router ──────────────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    try {
      if (url.pathname === "/webhook" && request.method === "POST") {
        return await handleWebhook(request, env);
      }
      if (url.pathname === "/webhook/undesignate" && request.method === "POST") {
        return await handleUndesignate(request, env);
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
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("Webhook payload:", JSON.stringify(payload).slice(0, 2000));

  const bookings = Array.isArray(payload) ? payload : [payload];

  let stored = 0;
  for (const booking of bookings) {
    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    console.log(`Designate: id=${trip.id} vehicle=${trip.vehicleRef} passenger=${trip.passenger} date=${trip.date}`);

    const key = `trips:${trip.date}`;
    const existing = await getTripsFromKV(env, key);

    // Upsert by trip ID, fallback to passenger+time match
    let idx = trip.id ? existing.findIndex((t) => t.id === trip.id) : -1;

    if (idx < 0 && trip.passenger && trip.time) {
      idx = existing.findIndex((t) =>
        t.passenger === trip.passenger && t.time === trip.time && t.id !== trip.id
      );
      if (idx >= 0) {
        console.log(`Reassignment detected: ${trip.passenger} moved from vehicle ${existing[idx].vehicleRef} to ${trip.vehicleRef}`);
        // Delete old Firestore doc if the ID changed
        firestoreDeleteTrip(env, existing[idx]).catch(e => console.error("Firestore cleanup error:", e));
      }
    }

    if (idx >= 0) {
      existing[idx] = trip;
    } else {
      existing.push(trip);
    }

    // Write to KV (fast cache)
    await env.TRIPS.put(key, JSON.stringify(existing), {
      expirationTtl: SEVEN_DAYS,
    });

    // Write to Firestore (source of truth) — fire and forget
    firestoreWriteTrip(env, trip).catch(e => console.error("Firestore write error:", e));

    stored++;
  }

  return jsonResponse({ ok: true, stored });
}

// ── Undesignate Handler ─────────────────────────────────────────────────────

async function handleUndesignate(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("Undesignate payload:", JSON.stringify(payload).slice(0, 2000));
  const bookings = Array.isArray(payload) ? payload : [payload];

  let removed = 0;
  for (const raw of bookings) {
    const booking = raw.booking || raw.data || raw;

    const id = String(
      booking.perma_id || booking.id || booking.booking_id || ""
    ).trim();

    const vehicleRef = String(
      booking.vehicle_number || booking.driver?.vehicle?.ref ||
      booking.vehicle_ref || booking.vehicleRef || booking.vehicle_id || ""
    ).trim();

    const passenger = booking.client_name || booking.name ||
      booking.passenger_name || booking.customer_name || "";

    if (!id && !passenger) {
      console.log("Undesignate: no id or passenger to match, skipping");
      continue;
    }

    console.log(`Undesignate: id=${id} vehicle=${vehicleRef} passenger=${passenger}`);

    const rawDate =
      booking.job_date ||
      booking.scheduled_pickup_date ||
      booking.pickup_date ||
      booking.date ||
      booking.release_date ||
      "";

    let date = extractDate(rawDate);

    if (date) {
      const result = await removeTripFromDate(env, date, id, vehicleRef, passenger);
      if (result.deleted) removed++;
    } else {
      const today = new Date();
      for (let i = 0; i < 8; i++) {
        const d = new Date(today);
        d.setDate(d.getDate() + i);
        const dateStr = d.toISOString().split("T")[0];
        const result = await removeTripFromDate(env, dateStr, id, vehicleRef, passenger);
        if (result.deleted) { removed++; break; }
      }
    }
  }

  console.log(`Undesignate: removed ${removed} trip(s)`);
  return jsonResponse({ ok: true, removed });
}

async function removeTripFromDate(env, date, tripId, vehicleRef, passenger) {
  const key = `trips:${date}`;
  const existing = await getTripsFromKV(env, key);
  const before = existing.length;

  const removedTrips = [];
  const filtered = existing.filter((t) => {
    // Match by trip ID (primary)
    if (tripId && t.id === tripId) { removedTrips.push(t); return false; }
    // Match by passenger + vehicle
    if (passenger && vehicleRef && t.passenger === passenger && t.vehicleRef === vehicleRef) { removedTrips.push(t); return false; }
    // Match by just vehicle ref (if no passenger provided but vehicle matches)
    if (!passenger && vehicleRef && t.vehicleRef === vehicleRef && !tripId) { removedTrips.push(t); return false; }
    // Match by just passenger name (if no vehicle provided)
    if (passenger && !vehicleRef && !tripId && t.passenger === passenger) { removedTrips.push(t); return false; }
    return true;
  });

  console.log(`removeTripFromDate: key=${key}, existing=${before}, matched=${removedTrips.length}, tripId=${tripId}, vehicle=${vehicleRef}, passenger=${passenger}`);

  if (filtered.length < before) {
    console.log(`Removed ${before - filtered.length} trip(s) from ${key}`);

    // Update KV
    if (filtered.length === 0) {
      await env.TRIPS.delete(key);
    } else {
      await env.TRIPS.put(key, JSON.stringify(filtered), {
        expirationTtl: SEVEN_DAYS,
      });
    }

    // Delete from Firestore
    for (const trip of removedTrips) {
      firestoreDeleteTrip(env, trip).catch(e => console.error("Firestore delete error:", e));
    }

    return { deleted: true };
  }

  return { deleted: false };
}

function extractDate(rawDate) {
  if (!rawDate) return "";
  try {
    const dt = new Date(rawDate);
    if (!isNaN(dt.getTime())) return dt.toISOString().split("T")[0];
  } catch (e) { /* ignore */ }
  const isoMatch = String(rawDate).match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];
  const dmyMatch = String(rawDate).match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (dmyMatch) return `${dmyMatch[3]}-${dmyMatch[2].padStart(2, "0")}-${dmyMatch[1].padStart(2, "0")}`;
  return "";
}

// ── Trips API (still serves from KV for backwards compatibility) ─────────

async function handleGetTrips(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;

  if (!from) {
    return jsonResponse({ error: "Missing 'from' query parameter (YYYY-MM-DD)" }, 400);
  }

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
  const booking = raw.booking || raw.data || raw;

  const id = String(
    booking.perma_id || booking.id || booking.booking_id || ""
  ).trim();

  const rawDate =
    booking.job_date ||
    booking.scheduled_pickup_date ||
    booking.pickup_date ||
    booking.date ||
    booking.release_date ||
    "";

  let date = extractDate(rawDate);
  if (!date) {
    date = new Date().toISOString().split("T")[0];
  }

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
      booking.address ||
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
