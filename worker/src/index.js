/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook              — receives iCabbi "Pre-booking: Driver Designated" events
 * POST /webhook/undesignate  — receives iCabbi "Driver Undesignate" events (deletes trip)
 * POST /webhook/update       — receives iCabbi "Pre-booking: Update" events (patches vehicle/driver)
 * GET  /trips                — returns stored trips for a date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
 * GET  /trips/stats          — quick count of stored trips for a date
 *
 * Storage: Supabase (source of truth, strong consistency) + Firestore (fire-and-forget backup)
 * Supabase table: trips (each trip is a row, atomic UPSERT/DELETE)
 * Firestore path: organizations/{orgId}/trips/{docId}
 */

const ORG_ID = "dispatch_team_main";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Webhook-Secret",
};

// ── Supabase REST API helpers ───────────────────────────────────────────────

function tripToRow(trip) {
  return {
    id: trip.id,
    date: trip.date,
    time: trip.time || "",
    vehicle_ref: trip.vehicleRef || "",
    pickup: trip.pickup || "",
    dropoff: trip.dropoff || "",
    passenger: trip.passenger || "",
    passenger_phone: trip.passengerPhone || "",
    driver_name: trip.driverName || "",
    driver_phone: trip.driverPhone || "",
    notes: trip.notes || "",
    status: trip.status || "",
    received_at: trip.receivedAt || new Date().toISOString(),
  };
}

function rowToTrip(row) {
  return {
    id: row.id,
    date: row.date,
    time: row.time || "",
    vehicleRef: row.vehicle_ref || "",
    pickup: row.pickup || "",
    dropoff: row.dropoff || "",
    passenger: row.passenger || "",
    passengerPhone: row.passenger_phone || "",
    driverName: row.driver_name || "",
    driverPhone: row.driver_phone || "",
    notes: row.notes || "",
    status: row.status || "",
    receivedAt: row.received_at || "",
  };
}

async function supabaseRequest(env, path, options = {}) {
  const baseUrl = (env.SUPABASE_URL || "").trim().replace(/\/+$/, "");
  const url = `${baseUrl}/rest/v1/${path}`;
  const headers = {
    apikey: env.SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
    "Content-Type": "application/json",
    ...options.headers,
  };

  const res = await fetch(url, { ...options, headers });
  return res;
}

async function supabaseUpsertTrip(env, trip) {
  const row = tripToRow(trip);
  const res = await supabaseRequest(env, "trips", {
    method: "POST",
    headers: {
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(row),
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase upsert failed for ${trip.id}:`, errText);
    return false;
  }
  console.log(`Supabase: upserted trip ${trip.id}`);
  return true;
}

async function supabaseDeleteById(env, tripId) {
  const res = await supabaseRequest(env, `trips?id=eq.${encodeURIComponent(tripId)}`, {
    method: "DELETE",
    headers: { Prefer: "return=representation" },
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase delete failed for ${tripId}:`, errText);
    return [];
  }
  const deleted = await res.json();
  if (deleted.length > 0) console.log(`Supabase: deleted trip ${tripId}`);
  return deleted;
}

async function supabaseDeleteByMatch(env, filters) {
  // Build PostgREST filter string
  const parts = [];
  for (const [col, val] of Object.entries(filters)) {
    parts.push(`${col}=eq.${encodeURIComponent(val)}`);
  }
  const query = parts.join("&");

  const res = await supabaseRequest(env, `trips?${query}`, {
    method: "DELETE",
    headers: { Prefer: "return=representation" },
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase delete by match failed:`, errText);
    return [];
  }
  const deleted = await res.json();
  if (deleted.length > 0) console.log(`Supabase: deleted ${deleted.length} trip(s) by match`);
  return deleted;
}

async function supabaseGetTrips(env, from, to) {
  const res = await supabaseRequest(env, `trips?date=gte.${from}&date=lte.${to}&order=date,time`, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase get trips failed:`, errText);
    return [];
  }
  return await res.json();
}

// ── Firestore REST API helpers ───────────────────────────────────────────────

let cachedAuthToken = null;
let tokenExpiresAt = 0;

async function getFirebaseIdToken(env) {
  if (cachedAuthToken && Date.now() < tokenExpiresAt - 300000) {
    return cachedAuthToken;
  }

  const apiKey = env.FIREBASE_API_KEY;
  if (!apiKey) {
    console.warn("FIREBASE_API_KEY not set — Firestore writes disabled");
    return null;
  }

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
  tokenExpiresAt = Date.now() + 3600000;
  return cachedAuthToken;
}

function tripToFirestoreDoc(trip) {
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

function getTripDocId(trip) {
  if (trip.id) return trip.id;
  // Vehicle is NOT part of the ID — it's a mutable attribute that can change.
  // Trip identity = date + time + passenger + pickup + dropoff.
  // Return trips differ by pickup/dropoff. Different times = different trips.
  const raw = `${trip.date}_${trip.time}_${trip.passenger}_${trip.pickup}_${trip.dropoff}`;
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
      if (url.pathname === "/webhook/update" && request.method === "POST") {
        return await handleUpdate(request, env);
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

    // Ensure we have a stable ID
    if (!trip.id) {
      trip.id = getTripDocId(trip);
    }

    console.log(`Designate: id=${trip.id} vehicle=${trip.vehicleRef} passenger=${trip.passenger} date=${trip.date} time=${trip.time}`);

    // Atomic UPSERT into Supabase — no read-modify-write needed.
    // Since vehicleRef is NOT part of the trip ID, reassigning a trip to a
    // different vehicle naturally updates the existing row via UPSERT.
    const ok = await supabaseUpsertTrip(env, trip);
    if (ok) stored++;

    // Write to Firestore (fire-and-forget backup)
    firestoreWriteTrip(env, trip).catch((e) => console.error("Firestore write error:", e));
  }

  return jsonResponse({ ok: true, stored });
}

// ── Update Handler (Pre-booking: Update) ────────────────────────────────────
// Catches edits like vehicle/driver assignment that don't trigger DESIGNATE.
// Uses the full normalizeTrip so the UPSERT updates all fields on the
// existing row (matched by stable trip ID).

async function handleUpdate(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("Update payload:", JSON.stringify(payload).slice(0, 2000));

  const bookings = Array.isArray(payload) ? payload : [payload];

  let updated = 0;
  for (const booking of bookings) {
    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    if (!trip.id) {
      trip.id = getTripDocId(trip);
    }

    console.log(`Update: id=${trip.id} vehicle=${trip.vehicleRef} passenger=${trip.passenger} date=${trip.date} time=${trip.time}`);

    // UPSERT — if the trip already exists (same ID), this updates all fields
    // including vehicleRef. If it's a new trip we haven't seen, it gets created.
    const ok = await supabaseUpsertTrip(env, trip);
    if (ok) updated++;

    firestoreWriteTrip(env, trip).catch((e) => console.error("Firestore write error:", e));
  }

  return jsonResponse({ ok: true, updated });
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

    const date = extractDate(rawDate);
    const result = await removeTripFromSupabase(env, id, vehicleRef, passenger, date);
    if (result.deleted) removed += result.count;
  }

  console.log(`Undesignate: removed ${removed} trip(s)`);
  return jsonResponse({ ok: true, removed });
}

async function removeTripFromSupabase(env, tripId, vehicleRef, passenger, date) {
  let deletedRows = [];

  // 1. Try by trip ID (most specific)
  if (tripId) {
    deletedRows = await supabaseDeleteById(env, tripId);
    if (deletedRows.length > 0) {
      for (const row of deletedRows) {
        firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
      }
      return { deleted: true, count: deletedRows.length };
    }
  }

  // 2. Try by passenger + vehicle + date (if we have a date)
  if (passenger && vehicleRef && date) {
    deletedRows = await supabaseDeleteByMatch(env, { passenger, vehicle_ref: vehicleRef, date });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) {
        firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
      }
      return { deleted: true, count: deletedRows.length };
    }
  }

  // 3. Try by passenger + vehicle (no date)
  if (passenger && vehicleRef) {
    deletedRows = await supabaseDeleteByMatch(env, { passenger, vehicle_ref: vehicleRef });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) {
        firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
      }
      return { deleted: true, count: deletedRows.length };
    }
  }

  // 4. Try by vehicle only (if no passenger)
  if (!passenger && vehicleRef) {
    const dateFilter = date || new Date().toISOString().split("T")[0];
    deletedRows = await supabaseDeleteByMatch(env, { vehicle_ref: vehicleRef, date: dateFilter });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) {
        firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
      }
      return { deleted: true, count: deletedRows.length };
    }
  }

  // 5. Try by passenger only (if no vehicle)
  if (passenger && !vehicleRef) {
    const filters = { passenger };
    if (date) filters.date = date;
    deletedRows = await supabaseDeleteByMatch(env, filters);
    if (deletedRows.length > 0) {
      for (const row of deletedRows) {
        firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
      }
      return { deleted: true, count: deletedRows.length };
    }
  }

  // 6. Scan upcoming 8 days if no date was provided and nothing matched yet
  if (!date) {
    const today = new Date();
    for (let i = 0; i < 8; i++) {
      const d = new Date(today);
      d.setDate(d.getDate() + i);
      const dateStr = d.toISOString().split("T")[0];
      const filters = { date: dateStr };
      if (passenger) filters.passenger = passenger;
      if (vehicleRef) filters.vehicle_ref = vehicleRef;

      deletedRows = await supabaseDeleteByMatch(env, filters);
      if (deletedRows.length > 0) {
        for (const row of deletedRows) {
          firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error("Firestore delete error:", e));
        }
        return { deleted: true, count: deletedRows.length };
      }
    }
  }

  return { deleted: false, count: 0 };
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

// ── Trips API (reads from Supabase for backward compatibility) ──────────────

async function handleGetTrips(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;

  if (!from) {
    return jsonResponse({ error: "Missing 'from' query parameter (YYYY-MM-DD)" }, 400);
  }

  const rows = await supabaseGetTrips(env, from, to);
  const dates = getDateRange(from, to);

  // Group by date for backward-compatible response format
  const allTrips = {};
  for (const row of rows) {
    const trip = rowToTrip(row);
    if (!allTrips[trip.date]) allTrips[trip.date] = [];
    allTrips[trip.date].push(trip);
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

  const rows = await supabaseGetTrips(env, from, to);
  const dates = getDateRange(from, to);

  // Count by date
  const stats = {};
  let total = 0;
  for (const date of dates) {
    stats[date] = 0;
  }
  for (const row of rows) {
    const d = row.date;
    stats[d] = (stats[d] || 0) + 1;
    total++;
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
