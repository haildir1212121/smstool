/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook               — receives iCabbi "Pre-booking: Driver Designated" events
 * POST /webhook/undesignate   — receives iCabbi "Driver Undesignate" events (deletes trip)
 * POST /webhook/update        — receives iCabbi "Pre-booking: Update" events (patches vehicle/driver)
 * POST /webhook/cancelled     — receives iCabbi "Pre-booking: Cancelled" events (marks trip cancelled)
 * POST /webhook/fleetio       — receives Fleetio issue reports, auto-creates iCabbi trips
 * GET  /trips                — returns stored trips for a date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
 * GET  /trips/stats          — quick count of stored trips for a date
 *
 * Storage: Supabase (source of truth, strong consistency) + Firestore (fire-and-forget backup)
 * Supabase table: trips (each trip is a row, atomic UPSERT/DELETE)
 * Firestore path: organizations/{orgId}/trips/{docId}
 *
 * Fleetio → iCabbi Pipeline:
 * 1. Driver reports issue in Fleetio → webhook fires here
 * 2. Worker maps issue + site_ref to repair shop → creates iCabbi trip
 * 3. Writes notification to Firestore for the SMS platform UI
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
    site_ref: trip.siteRef || "", // ✅ ADD THIS
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
    siteRef: row.site_ref || "", // ✅ ADD THIS
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

// ── Driver Shift Supabase helpers ────────────────────────────────────────────

async function supabaseUpsertShift(env, shift) {
  const res = await supabaseRequest(env, "driver_shifts", {
    method: "POST",
    headers: { Prefer: "resolution=merge-duplicates,return=minimal" },
    body: JSON.stringify(shift),
  });
  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase upsert shift failed for ${shift.id}:`, errText);
    return { ok: false, error: errText };
  }
  console.log(`Supabase: upserted shift ${shift.id}`);
  return { ok: true };
}

async function supabaseUpdateShift(env, id, updates) {
  const res = await supabaseRequest(env, `driver_shifts?id=eq.${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { Prefer: "return=minimal", "Content-Type": "application/json" },
    body: JSON.stringify(updates),
  });
  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase update shift failed for ${id}:`, errText);
    return false;
  }
  return true;
}

async function supabaseGetShifts(env, from, to) {
  const res = await supabaseRequest(env, `driver_shifts?date=gte.${from}&date=lte.${to}&order=date,driver_name`, {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const errText = await res.text();
    console.error(`Supabase get shifts failed:`, errText);
    return [];
  }
  return await res.json();
}

async function supabaseGetOpenShift(env, driverName, date) {
  const res = await supabaseRequest(
    env,
    `driver_shifts?driver_name=ilike.${encodeURIComponent(driverName)}&date=eq.${date}&signed_out_at=is.null&order=created_at.desc&limit=1`,
    { method: "GET", headers: { Accept: "application/json" } }
  );
  if (!res.ok) return null;
  const rows = await res.json();
  return rows[0] || null;
}

async function supabaseGetDriverTripsForDate(env, driverName, date) {
  const res = await supabaseRequest(
    env,
    `trips?driver_name=ilike.${encodeURIComponent(driverName)}&date=eq.${date}&order=time`,
    { method: "GET", headers: { Accept: "application/json" } }
  );
  if (!res.ok) return [];
  return await res.json();
}

// Find the most recent shift for a vehicle on a given date (used to match sign-in to pre-seeded shift)
async function supabaseGetExpectedShift(env, vehicleRef, date) {
  const res = await supabaseRequest(
    env,
    `driver_shifts?vehicle_ref=eq.${encodeURIComponent(vehicleRef)}&date=eq.${date}&order=created_at.desc&limit=1`,
    { method: "GET", headers: { Accept: "application/json" } }
  );
  if (!res.ok) return null;
  const rows = await res.json();
  return rows[0] || null;
}

// Find the most recent shift for a driver_ref on a given date (used to match sign-out)
async function supabaseGetShiftByDriverRef(env, driverRef, date) {
  const res = await supabaseRequest(
    env,
    `driver_shifts?driver_ref=eq.${encodeURIComponent(driverRef)}&date=eq.${date}&order=created_at.desc&limit=1`,
    { method: "GET", headers: { Accept: "application/json" } }
  );
  if (!res.ok) return null;
  const rows = await res.json();
  return rows[0] || null;
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
      if (url.pathname === "/webhook/cancelled" && request.method === "POST") {
        return await handleBookingCancelled(request, env);
      }
      if (url.pathname === "/webhook/fleetio" && request.method === "POST") {
        return await handleFleetioWebhook(request, env);
      }
      if (url.pathname === "/webhook/created" && request.method === "POST") {
  return await handleBookingCreated(request, env);
}
      if (url.pathname === "/trips" && request.method === "GET") {
        return await handleGetTrips(request, env);
      }
      if (url.pathname === "/trips/stats" && request.method === "GET") {
        return await handleTripStats(request, env);
      }
      if (url.pathname === "/webhook/driver/signin" && request.method === "POST") {
        return await handleDriverSignIn(request, env);
      }
      if (url.pathname === "/webhook/driver/signout" && request.method === "POST") {
        return await handleDriverSignOut(request, env);
      }
      if (url.pathname === "/shifts" && request.method === "GET") {
        return await handleGetShifts(request, env);
      }
      if (url.pathname === "/shifts/seed" && request.method === "POST") {
        return await handleSeedShifts(request, env);
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
  // iCabbi "Pre-booking: Driver Designated"
  return await processTripEvent(request, env, "designate");
}

async function handleUpdate(request, env) {
  // iCabbi "Pre-booking: Update"
  return await processTripEvent(request, env, "update");
}

async function handleUndesignate(request, env) {
  // iCabbi "Driver Undesignate"
  return await processTripEvent(request, env, "undesignate");
}

async function handleBookingCancelled(request, env) {
  // iCabbi "Pre-booking: Cancelled" — fired when any cancellation occurs (operator, API, passenger app)
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  const bookings = Array.isArray(payload) ? payload : [payload];

  let processed = 0;

  for (const booking of bookings) {
    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    // Stamp cancelled status and upsert — keeps the row visible in the dispatcher view
    // so they know the booking existed and was cancelled (not just silently missing).
    trip.status = "cancelled";

    console.log(`[CANCELLED] Marking trip ${trip.id} as cancelled (passenger: ${trip.passenger}, vehicle: ${trip.vehicleRef || "unassigned"}).`);

    const ok = await supabaseUpsertTrip(env, trip);
    if (ok) {
      processed++;

      const label = [trip.passenger, trip.pickup, trip.date].filter(Boolean).join(" · ");
      writeNotificationToFirestore(env, {
        type: "trip_cancelled",
        title: `Trip Cancelled${trip.vehicleRef ? ` — Vehicle ${trip.vehicleRef}` : ""}`,
        message: label || trip.id,
        vehicleRef: trip.vehicleRef || "",
        driverName: trip.driverName || "",
        tripDate: trip.date || "",
      }).catch(e => console.error("Firestore cancel notification error:", e));

      // Remove from Firestore trip backup since the booking is no longer active
      firestoreDeleteTrip(env, trip).catch(e => console.error("Firestore cancel delete error:", e));
    }
  }

  return jsonResponse({ ok: true, processed });
}

// ── Unified Processing Logic ──────────────────────────────────────────────────

async function processTripEvent(request, env, eventType) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  const bookings = Array.isArray(payload) ? payload : [payload];

  let processedCount = 0;
  
  for (const booking of bookings) {
    // 1. Normalize the trip using the deterministic ID logic so we ALWAYS target the exact same row
    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    // 2. Is this an explicit removal, or an update with a cleared driver?
    if (eventType === "undesignate" || !trip.vehicleRef) {
      console.log(`[${eventType.toUpperCase()}] Removing trip ${trip.id} from database (Unassigned).`);

      const result = await removeTripFromSupabase(env, trip.id, trip.vehicleRef, trip.passenger, trip.date);
      if (result && result.deleted) {
        processedCount++;
        const label = [trip.passenger, trip.pickup, trip.date].filter(Boolean).join(" · ");
        writeNotificationToFirestore(env, {
          type: "trip_undesignated",
          title: `Trip Undesignated${trip.vehicleRef ? ` — Vehicle ${trip.vehicleRef}` : ""}`,
          message: label || trip.id,
          vehicleRef: trip.vehicleRef || "",
          driverName: trip.driverName || "",
          tripDate: trip.date || "",
        }).catch(e => console.error("Firestore undesignate notification error:", e));
      }
      continue;
    }

    // 3. If it has a driver/vehicle, we UPSERT.
    // This perfectly handles: No Driver -> Driver, AND Driver -> New Driver
    console.log(`[${eventType.toUpperCase()}] Upserting trip ${trip.id} (Vehicle: ${trip.vehicleRef}).`);
    const ok = await supabaseUpsertTrip(env, trip);
    if (ok) {
      processedCount++;
      const label = [trip.passenger, trip.pickup, trip.date].filter(Boolean).join(" · ");
      writeNotificationToFirestore(env, {
        type: "trip_designated",
        title: `Trip Designated — Vehicle ${trip.vehicleRef}`,
        message: label || trip.id,
        vehicleRef: trip.vehicleRef || "",
        driverName: trip.driverName || "",
        tripDate: trip.date || "",
      }).catch(e => console.error("Firestore designate notification error:", e));
    }

    // Backup to Firestore
    firestoreWriteTrip(env, trip).catch((e) => console.error("Firestore write error:", e));
  }

  return jsonResponse({ ok: true, processed: processedCount });
}

async function removeTripFromSupabase(env, tripId, vehicleRef, passenger, date) {
  let deletedRows = [];
  if (tripId) {
    deletedRows = await supabaseDeleteById(env, tripId);
    if (deletedRows.length > 0) {
      for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
      return { deleted: true, count: deletedRows.length };
    }
  }
  if (passenger && vehicleRef && date) {
    deletedRows = await supabaseDeleteByMatch(env, { passenger, vehicle_ref: vehicleRef, date });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
      return { deleted: true, count: deletedRows.length };
    }
  }
  if (passenger && vehicleRef) {
    deletedRows = await supabaseDeleteByMatch(env, { passenger, vehicle_ref: vehicleRef });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
      return { deleted: true, count: deletedRows.length };
    }
  }
  if (!passenger && vehicleRef) {
    const dateFilter = date || new Date().toISOString().split("T")[0];
    deletedRows = await supabaseDeleteByMatch(env, { vehicle_ref: vehicleRef, date: dateFilter });
    if (deletedRows.length > 0) {
      for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
      return { deleted: true, count: deletedRows.length };
    }
  }
  if (passenger && !vehicleRef) {
    const filters = { passenger };
    if (date) filters.date = date;
    deletedRows = await supabaseDeleteByMatch(env, filters);
    if (deletedRows.length > 0) {
      for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
      return { deleted: true, count: deletedRows.length };
    }
  }
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
        for (const row of deletedRows) firestoreDeleteTrip(env, rowToTrip(row)).catch((e) => console.error(e));
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
  } catch (e) { }
  const isoMatch = String(rawDate).match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];
  const dmyMatch = String(rawDate).match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (dmyMatch) return `${dmyMatch[3]}-${dmyMatch[2].padStart(2, "0")}-${dmyMatch[1].padStart(2, "0")}`;
  return "";
}

async function handleGetTrips(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;
  if (!from) return jsonResponse({ error: "Missing from param" }, 400);
  const rows = await supabaseGetTrips(env, from, to);
  const dates = getDateRange(from, to);
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
  if (!from) return jsonResponse({ error: "Missing from param" }, 400);
  const rows = await supabaseGetTrips(env, from, to);
  const dates = getDateRange(from, to);
  const stats = {};
  let total = 0;
  for (const date of dates) stats[date] = 0;
  for (const row of rows) {
    stats[row.date] = (stats[row.date] || 0) + 1;
    total++;
  }
  return jsonResponse({ from, to, total, byDate: stats });
}

// ── Fleetio Webhook Handler ─────────────────────────────────────────────────

async function handleFleetioWebhook(request, env) {
  const signature = request.headers.get("x-fleetio-webhook-signature") || "";
  const expectedSecret = env.FLEETIO_WEBHOOK_SECRET || env.WEBHOOK_SECRET;

  if (!expectedSecret) {
    console.error("FLEETIO_WEBHOOK_SECRET not set");
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const rawBody = await request.text();

  if (signature) {
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey("raw", encoder.encode(expectedSecret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const sig = await crypto.subtle.sign("HMAC", key, encoder.encode(rawBody));
    const computed = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, "0")).join("");
    if (computed !== signature) return jsonResponse({ error: "Unauthorized" }, 401);
  } else {
    const authHeader = request.headers.get("Authorization") || "";
    const rawSecret = authHeader.replace(/^(Bearer|Token)\s+/i, "").trim();
    if (!rawSecret || rawSecret !== expectedSecret) return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = JSON.parse(rawBody);
  const issue = normalizeFleetioIssue(payload);

  if (!issue || !issue.driverName) {
    await writeNotificationToFirestore(env, {
      type: "fleetio_issue",
      title: `Fleetio Issue: ${(issue?.description || "Unknown").slice(0, 60)}`,
      message: `No driver name found. Vehicle: ${issue?.vehicleRef || "Unknown"}.`,
      vehicleRef: issue?.vehicleRef || "",
      driverName: "",
      pipelineSteps: [{ label: "Issue Reported", status: "done" }, { label: "Driver Match", status: "pending" }, { label: "Trip Creation", status: "pending" }, { label: "Notification", status: "done" }]
    });
    return jsonResponse({ ok: true, action: "notification_only", reason: "no_driver_name" });
  }

  const contact = await findContactByName(env, issue.driverName);

  if (!contact) {
    await writeNotificationToFirestore(env, {
      type: "fleetio_issue",
      title: `Fleetio Issue: ${issue.description.slice(0, 60)}`,
      message: `Driver "${issue.driverName}" not found in SMS contacts. Vehicle: ${issue.vehicleRef || "Unknown"}. Trip not auto-created.`,
      vehicleRef: issue.vehicleRef || "",
      driverName: issue.driverName,
      pipelineSteps: [{ label: "Issue Reported", status: "done" }, { label: "Driver Match", status: "pending" }, { label: "Trip Creation", status: "pending" }, { label: "Notification", status: "done" }]
    });
    return jsonResponse({ ok: true, action: "notification_only", reason: "driver_not_found" });
  }

  const settings = await loadAutomationSettings(env);
  const repairShop = findRepairShop(settings, issue.siteRef, issue.description);
  const tripName = buildTripName(settings, issue);

  // Time Fix: 23:59 PDT
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const bookingTimePST = `${year}-${month}-${day}T23:59:00`;

  let icabbiResult = null;
  const pickupAddress = repairShop?.address || "Repair Shop (Address TBD)";

  const icabbiBody = {
    date: bookingTimePST,
    name: tripName,
    phone: "",
    address: {
      lat: 44.0521,
      lng: -123.0868,
      formatted: pickupAddress
    },
    destination: {
      lat: 44.0521,
      lng: -123.0868,
      formatted: "Vehicle Home Base"
    },
    driver_instructions: `Fleetio: ${issue.description}. Vehicle: ${issue.vehicleRef || "N/A"}.`
  };

  if (env.ICABBI_AUTH) {
    try {
      icabbiResult = await createIcabbiTrip(env, icabbiBody);
    } catch (e) {
      icabbiResult = { error: e.message };
    }
  } else {
    icabbiResult = { skipped: true, reason: "icabbi_auth_not_configured" };
  }

  const tripCreated = icabbiResult && !icabbiResult.error && !icabbiResult.skipped;

  const localTrip = {
    id: `fleetio_${issue.vehicleRef || "unknown"}_${Date.now()}`,
    date: `${year}-${month}-${day}`,
    time: "23:59",
    vehicleRef: issue.vehicleRef || "",
    pickup: pickupAddress,
    dropoff: "",
    passenger: tripName,
    passengerPhone: contact.phone || "",
    driverName: contact.name || issue.driverName,
    driverPhone: contact.phone || "",
    notes: icabbiBody.driver_instructions,
    status: tripCreated ? "auto_created" : "pending_manual",
    receivedAt: now.toISOString()
  };
  await supabaseUpsertTrip(env, localTrip);

  return jsonResponse({ ok: true, tripCreated, tripName, driverName: contact.name, vehicleRef: issue.vehicleRef });
}

// ── Helpers & Lookups ────────────────────────────────────────────────────────

async function findContactByName(env, driverName) {
  const token = await getFirebaseIdToken(env);
  if (!token) return null;
  const res = await fetch(`https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID || "sms-dlx"}/databases/(default)/documents/organizations/${ORG_ID}/threads?key=${env.FIREBASE_API_KEY}&pageSize=500`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) return null;
  const data = await res.json();
  const documents = data.documents || [];
  const searchName = driverName.toLowerCase().trim();
  const searchParts = searchName.split(/\s+/);
  let bestMatch = null;
  for (const doc of documents) {
    const name = (doc.fields?.name?.stringValue || "").trim().toLowerCase();
    const phone = (doc.fields?.phone?.stringValue || doc.name?.split("/").pop() || "").trim();
    if (!name || !phone) continue;
    if (name === searchName) return { name: doc.fields.name.stringValue, phone };
    const contactParts = name.split(/\s+/);
    if (searchParts.length >= 2 && contactParts.length >= 2) {
      if (searchParts[searchParts.length - 1] === contactParts[contactParts.length - 1] && (contactParts[0].startsWith(searchParts[0]) || searchParts[0].startsWith(contactParts[0]))) bestMatch = { name: doc.fields.name.stringValue, phone };
    }
    if (!bestMatch && (name.includes(searchName) || searchName.includes(name))) bestMatch = { name: doc.fields.name.stringValue, phone };
  }
  return bestMatch;
}

function normalizeFleetioIssue(raw) {
  const data = raw.payload || raw.data || raw;
  const reporter = data.reported_by || {};
  return {
    vehicleRef: String(data.vehicle_name || data.vehicle_number || "").trim(),
    siteRef: String(reporter.group_name || data.location_name || "").trim(),
    description: String(data.name || data.summary || data.description || "").trim(),
    driverName: String(data.reported_by_name || reporter.name || "").trim(),
    driverPhone: String(reporter.mobile_phone_number || "").trim(),
    driverEmail: String(reporter.email || "").trim(),
    fleetioId: String(data.id || "").trim(),
    eventType: String(raw.event || "").trim()
  };
}

async function loadAutomationSettings(env) {
  const token = await getFirebaseIdToken(env);
  if (!token) return { repairMappings: [], issueMappings: [] };
  const res = await fetch(`https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID || "sms-dlx"}/databases/(default)/documents/organizations/${ORG_ID}/settings/automation?key=${env.FIREBASE_API_KEY}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) return buildSettingsFromEnv(env);
  const doc = await res.json();
  const fields = doc.fields || {};
  return { repairMappings: parseFirestoreArray(fields.repairMappings), issueMappings: parseFirestoreArray(fields.issueMappings) };
}

function parseFirestoreArray(field) {
  if (!field?.arrayValue?.values) return [];

  return field.arrayValue.values.map(v => {
    const f = v.mapValue?.fields || {};
    const obj = {};

    for (const [k, val] of Object.entries(f)) {
      if (val.arrayValue) {
        obj[k] = val.arrayValue.values.map(x => x.stringValue);
      } else {
        obj[k] = val.stringValue || val.integerValue || "";
      }
    }

    return obj;
  });
}

function buildSettingsFromEnv(env) {
  const repairMappings = [];
  const issueMappings = [];
  if (env.REPAIR_MAPPINGS) {
    for (const entry of env.REPAIR_MAPPINGS.split(";")) {
      const [site, shop, addr] = entry.split(":");
      if (site) repairMappings.push({ siteRef: site.trim(), shopName: shop.trim(), address: addr.trim() });
    }
  }
  return { repairMappings, issueMappings };
}

function findRepairShop(settings, siteRef, description) {
  const normalized = (siteRef || "").toUpperCase().trim();

  // Match against groupNames array
  const match = settings.repairMappings.find(m => {
    if (!m.groupNames) return false;

    return m.groupNames.some(name =>
      normalized.includes(name.toUpperCase())
    );
  });

  if (match) return match;

  // fallback
  return settings.repairMappings.find(m =>
    m.groupNames?.includes("*") || m.groupNames?.includes("DEFAULT")
  ) || null;
}

// Fixed Trip Name Format
function buildTripName(settings, issue) {
  const issueName = issue.description || "Unknown Issue";
  const vehicleName = issue.vehicleRef || "Unknown Vehicle";
  return `${issueName} / ${vehicleName}`;
}

async function createIcabbiTrip(env, tripBody) {
  const apiUrl = (env.ICABBI_API_URL || "https://api.icabbi.us/us4").replace(/\/+$/, "");
  const res = await fetch(`${apiUrl}/bookings/add`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `basic ${env.ICABBI_AUTH}` },
    body: JSON.stringify(tripBody)
  });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

async function writeNotificationToFirestore(env, data) {
  const token = await getFirebaseIdToken(env);
  const url = `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID || "sms-dlx"}/databases/(default)/documents/organizations/${ORG_ID}/notifications?key=${env.FIREBASE_API_KEY}`;
  const fields = { read: { booleanValue: false }, createdAt: { timestampValue: new Date().toISOString() } };
  for (const [k, v] of Object.entries(data)) {
    if (Array.isArray(v)) {
      fields[k] = { arrayValue: { values: v.map(i => ({ mapValue: { fields: { label: { stringValue: i.label }, status: { stringValue: i.status } } } })) } };
    } else if (typeof v === "boolean") {
      fields[k] = { booleanValue: v };
    } else {
      fields[k] = { stringValue: String(v) };
    }
  }
  await fetch(url, { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` }, body: JSON.stringify({ fields }) });
}

function normalizeTrip(raw) {
  const b = raw.booking || raw.data || raw;

  const pickup =
    b.city || 
    b.pickup_address ||
    b.pickup_name ||
    b.pickup?.formatted ||
    b.pickup?.address ||
    b.pickup?.text ||
    b.pickup?.name ||
    b.pickup_area ||
    b.from ||
    "Unknown";

  const dropoff =
    b.dropoff_address ||
    b.destination_name ||
    b.destination?.formatted ||
    b.destination?.address ||
    b.destination?.text ||
    b.destination?.name ||
    b.to ||
    "";

  // Add booking_id to catch iCabbi's primary ID field
  const baseId = String(b.booking_id || b.perma_id || b.id || "").trim();
  const leg = String(b.leg_number || b.leg || b.sequence || "").trim();

  let id = baseId;

  if (baseId && leg) {
    id = `${baseId}_${leg}`;
  }

  if (!id) {
    // DETERMINISTIC FALLBACK: NO RANDOM NUMBERS.
    // Generates the exact same ID for the same trip updates.
    const safeTime = String(b.job_time || b.time || "notime").trim();
    const safePass = String(b.client_name || b.name || "nopass").trim().replace(/[^a-zA-Z0-9]/g, '');
    const safeDate = extractDate(b.job_date || b.date || "");
    
    id = `auto_${safeDate}_${safeTime}_${safePass}`;
  }

  return {
    id,
    date: extractDate(b.job_date || b.date || ""),
    time: b.job_time || b.time || "",
    vehicleRef: String(b.vehicle_number || "").trim(),
    pickup,
    dropoff,
    passenger: b.client_name || b.name || "",
    passengerPhone: b.phone || "",
    siteRef: String(b.site_ref || "").trim(), 
    driverName: [b.driver_first, b.driver_last].filter(Boolean).join(" "),
    driverPhone: b.driver_phone || "",
    notes: b.notes || b.driver_notes || "",
    status: b.status || "",
    receivedAt: new Date().toISOString(),
  };
}

function getDateRange(from, to) {
  const dates = [];
  const start = new Date(from + "T12:00:00Z");
  const end = new Date(to + "T12:00:00Z");
  if (isNaN(start.getTime())) return [from];
  const current = new Date(start);
  while (current <= end) { dates.push(current.toISOString().split("T")[0]); current.setDate(current.getDate() + 1); }
  return dates;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json", ...CORS_HEADERS } });
}

async function handleBookingCreated(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  const bookings = Array.isArray(payload) ? payload : [payload];

  let processed = 0;
  let skipped = 0;

  for (const booking of bookings) {
    const b = booking.booking || booking.data || booking;
    const siteRef = String(b.site_ref || "").trim().toUpperCase();

    if (siteRef === "ORTX") {
      skipped++;
      console.log(`[CREATED] Skipping ORTX booking`);
      continue;
    }

    const trip = normalizeTrip(booking);
    if (!trip || !trip.date) continue;

    trip.status = trip.status || "booked";

    console.log(`[CREATED] Upserting baseline trip ${trip.id} (site: ${siteRef})`);
    const ok = await supabaseUpsertTrip(env, trip);
    if (ok) processed++;

    firestoreWriteTrip(env, trip).catch((e) => console.error("Firestore write error:", e));
  }

  return jsonResponse({ ok: true, processed, skipped });
}

// ── Driver Sign-In / Sign-Out Handlers ───────────────────────────────────────

function isObj(v) { return v !== null && typeof v === "object" && !Array.isArray(v); }

// Extract fields from iCabbi login/logout webhook payload.
// iCabbi login/logout uses: #driver_ref, #vehicle_ref, #phone, #hook_sent_at
// Driver name is NOT sent — it comes from the pre-seeded driver_shifts row.
function extractDriverEventFields(payload) {
  const b = (isObj(payload.data)    && payload.data)
         || (isObj(payload.booking) && payload.booking)
         || (isObj(payload.driver)  && payload.driver)
         || (isObj(payload.event)   && payload.event)
         || payload;

  // Phone — iCabbi login/logout webhook uses #phone variable
  const phone = String(b.phone || b.driver_phone || b.mobile || payload.phone || payload.driver_phone || "").trim();

  // Vehicle — iCabbi login/logout webhook uses #vehicle_ref variable
  const vehicleRef = String(b.vehicle_ref || b.vehicle_number || b.vehicle_id || b.vehicle || payload.vehicle_ref || payload.vehicle_number || "").trim();

  // driver_ref — primary identifier for login/logout events
  const driverRef = String(b.driver_ref || b.driver_id || b.driver_ix || payload.driver_ref || payload.driver_id || "").trim();

  // Timestamp — login/logout webhook uses #hook_sent_at
  const rawTs = b.hook_sent_at || b.timestamp || b.event_time || b.login_time || b.logout_time
    || b.signed_in_at || b.signed_out_at || b.created_at
    || payload.hook_sent_at || payload.timestamp || new Date().toISOString();
  const ts = new Date(rawTs);
  const date = !isNaN(ts.getTime())
    ? ts.toISOString().split("T")[0]
    : (extractDate(b.job_date || b.date || payload.job_date || payload.date || "") || new Date().toISOString().split("T")[0]);

  return {
    driverPhone: phone,
    vehicleRef,
    driverRef,
    date,
    timestamp: !isNaN(ts.getTime()) ? ts.toISOString() : new Date().toISOString(),
  };
}

// Parse "HH:MM" or "H:MM AM/PM" into minutes since midnight
function parseTimeToMinutes(timeStr) {
  if (!timeStr) return null;
  const clean = timeStr.trim().toUpperCase();
  const ampm = /(\d{1,2}):(\d{2})\s*(AM|PM)/.exec(clean);
  if (ampm) {
    let h = parseInt(ampm[1], 10);
    const m = parseInt(ampm[2], 10);
    if (ampm[3] === "PM" && h !== 12) h += 12;
    if (ampm[3] === "AM" && h === 12) h = 0;
    return h * 60 + m;
  }
  const plain = /^(\d{1,2}):(\d{2})/.exec(clean);
  if (plain) return parseInt(plain[1], 10) * 60 + parseInt(plain[2], 10);
  return null;
}

async function handleDriverSignIn(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("[SIGNIN] Raw payload:", JSON.stringify(payload));

  const fields = extractDriverEventFields(payload);
  console.log("[SIGNIN] Extracted fields:", JSON.stringify(fields));

  const { vehicleRef, driverRef, date, timestamp: signedInAt } = fields;

  if (!vehicleRef && !driverRef) {
    return jsonResponse({ ok: false, reason: "no_vehicle_or_driver_ref", rawKeys: Object.keys(payload) });
  }

  const signedInTs = new Date(signedInAt);
  const signedInMin = signedInTs.getHours() * 60 + signedInTs.getMinutes();

  // Look up pre-seeded shift by vehicle_ref + date (created when dispatcher sends trip texts)
  let existingShift = null;
  if (vehicleRef) {
    existingShift = await supabaseGetExpectedShift(env, vehicleRef, date);
  }

  let isLate = false;
  let lateByMin = 0;
  let driverName = "";
  let shiftId = "";

  if (existingShift) {
    driverName = existingShift.driver_name;
    shiftId = existingShift.id;

    // Compare actual sign-in to scheduled tablet time from pre-seeded row
    const gracePeriodMin = parseInt(env.LATE_GRACE_MINUTES || "15", 10);
    const scheduledMin = parseTimeToMinutes(existingShift.scheduled_signin);
    if (scheduledMin !== null) {
      const diff = signedInMin - scheduledMin;
      if (diff > gracePeriodMin) { isLate = true; lateByMin = diff; }
    }

    console.log("[SIGNIN] Updating pre-seeded shift:", shiftId, "driver:", driverName);
    await supabaseUpdateShift(env, shiftId, {
      driver_ref: driverRef || "",
      driver_phone: fields.driverPhone || "",
      signed_in_at: signedInAt,
      is_late: isLate,
      late_by_min: lateByMin,
      status: "signed_in",
      updated_at: new Date().toISOString(),
    });
  } else {
    // No pre-seeded shift — driver logged in without being scheduled, create a new row
    driverName = driverRef ? `Driver #${driverRef}` : (vehicleRef ? `Vehicle ${vehicleRef}` : "Unknown");
    shiftId = vehicleRef
      ? `shift_${date}_${String(vehicleRef).replace(/\s+/g, "_")}`
      : `shift_${date}_ref_${driverRef}`;

    console.log("[SIGNIN] No pre-seeded shift found, creating new:", shiftId);
    await supabaseUpsertShift(env, {
      id: shiftId,
      driver_name: driverName,
      driver_ref: driverRef || "",
      driver_phone: fields.driverPhone || "",
      vehicle_ref: vehicleRef || "",
      date,
      scheduled_signin: "",
      scheduled_stop: "",
      signed_in_at: signedInAt,
      is_late: false,
      late_by_min: 0,
      status: "signed_in",
      go_home_sent: false,
      updated_at: new Date().toISOString(),
    });
  }

  if (isLate) {
    await writeNotificationToFirestore(env, {
      type: "late_signin",
      title: `Late Sign-In: ${driverName}`,
      message: `${driverName} signed in ${lateByMin} minute(s) late (scheduled ${existingShift?.scheduled_signin || "unknown"}, signed in at ${signedInTs.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}).`,
      driverName,
      driverPhone: fields.driverPhone,
      vehicleRef,
      shiftId,
      lateByMin: String(lateByMin),
    }).catch((e) => console.error("Firestore late_signin notification error:", e));
  }

  return jsonResponse({ ok: true, shiftId, isLate, lateByMin, driverName, foundExisting: !!existingShift });
}

async function handleDriverSignOut(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("[SIGNOUT] Raw payload:", JSON.stringify(payload));

  const fields = extractDriverEventFields(payload);
  console.log("[SIGNOUT] Extracted fields:", JSON.stringify(fields));

  const { vehicleRef, driverRef, date, timestamp: signedOutAt } = fields;

  if (!driverRef && !vehicleRef) {
    return jsonResponse({ ok: false, reason: "no_driver_ref_or_vehicle", rawKeys: Object.keys(payload) });
  }

  const signedOutTs = new Date(signedOutAt);

  // Find the shift — prefer driver_ref (stamped during sign-in), fall back to vehicle_ref
  let shift = null;
  if (driverRef) {
    shift = await supabaseGetShiftByDriverRef(env, driverRef, date);
  }
  if (!shift && vehicleRef) {
    shift = await supabaseGetExpectedShift(env, vehicleRef, date);
  }

  let totalHoursMin = null;
  let shiftId;
  let driverName = "";

  if (shift) {
    shiftId = shift.id;
    driverName = shift.driver_name;
    if (shift.signed_in_at) {
      const signedInTs = new Date(shift.signed_in_at);
      totalHoursMin = Math.round((signedOutTs - signedInTs) / 60000);
    }
    console.log("[SIGNOUT] Updating shift:", shiftId, "driver:", driverName, "totalHoursMin:", totalHoursMin);
    await supabaseUpdateShift(env, shiftId, {
      signed_out_at: signedOutAt,
      ...(totalHoursMin !== null ? { total_hours_min: totalHoursMin } : {}),
      status: "signed_out",
      updated_at: new Date().toISOString(),
    });
  } else {
    // No shift found — create a sign-out-only record
    driverName = driverRef ? `Driver #${driverRef}` : `Vehicle ${vehicleRef}`;
    shiftId = vehicleRef
      ? `shift_${date}_${String(vehicleRef).replace(/\s+/g, "_")}`
      : `shift_${date}_ref_${driverRef}`;
    console.log("[SIGNOUT] No shift found, creating sign-out-only record:", shiftId);
    await supabaseUpsertShift(env, {
      id: shiftId,
      driver_name: driverName,
      driver_ref: driverRef || "",
      driver_phone: fields.driverPhone || "",
      vehicle_ref: vehicleRef || "",
      date,
      scheduled_signin: "",
      scheduled_stop: "",
      signed_out_at: signedOutAt,
      is_late: false,
      late_by_min: 0,
      status: "signed_out",
      go_home_sent: false,
      updated_at: new Date().toISOString(),
    });
  }

  const maxHoursMin = parseInt(env.MAX_SHIFT_HOURS || "10", 10) * 60;
  if (totalHoursMin !== null && totalHoursMin >= maxHoursMin) {
    const hoursWorked = (totalHoursMin / 60).toFixed(1);
    await writeNotificationToFirestore(env, {
      type: "long_shift",
      title: `Long Shift: ${driverName}`,
      message: `${driverName} worked ${hoursWorked} hours today (vehicle ${vehicleRef || "N/A"}).`,
      driverName,
      driverPhone: fields.driverPhone,
      vehicleRef,
      shiftId,
      totalHoursMin: String(totalHoursMin),
    }).catch((e) => console.error("Firestore long_shift notification error:", e));
  }

  return jsonResponse({ ok: true, shiftId, totalHoursMin, signedOutAt, driverName });
}

async function handleGetShifts(request, env) {
  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to") || from;
  if (!from) return jsonResponse({ error: "Missing from param" }, 400);
  const rows = await supabaseGetShifts(env, from, to);
  return jsonResponse({ from, to, shifts: rows });
}

async function handleSeedShifts(request, env) {
  const secret = request.headers.get("X-Webhook-Secret") || "";
  if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const body = await request.json();
  const { date, shifts } = body;
  if (!date || !Array.isArray(shifts)) {
    return jsonResponse({ error: "Missing date or shifts array" }, 400);
  }

  let seeded = 0;
  for (const s of shifts) {
    if (!s.driver_name || !s.vehicle_ref) continue;
    const row = {
      id: `shift_${date}_${String(s.vehicle_ref).replace(/\s+/g, "_")}`,
      driver_name: s.driver_name,
      driver_ref: "",
      driver_phone: "",
      vehicle_ref: String(s.vehicle_ref),
      date,
      scheduled_signin: s.scheduled_signin || "",
      scheduled_stop: s.scheduled_stop || "",
      status: "expected",
      go_home_sent: false,
      updated_at: new Date().toISOString(),
    };
    const result = await supabaseUpsertShift(env, row);
    if (result.ok) seeded++;
  }

  return jsonResponse({ ok: true, seeded });
}
