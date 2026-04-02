/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook              — receives iCabbi "Pre-booking: Driver Designated" events
 * POST /webhook/undesignate  — receives iCabbi "Driver Undesignate" events (deletes trip)
 * POST /webhook/update       — receives iCabbi "Pre-booking: Update" events (patches vehicle/driver)
 * POST /webhook/fleetio      — receives Fleetio issue reports, auto-creates iCabbi trips
 * GET  /trips                — returns stored trips for a date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
 * GET  /trips/stats          — quick count of stored trips for a date
 *
 * Storage: Supabase (source of truth, strong consistency) + Firestore (fire-and-forget backup)
 * Supabase table: trips (each trip is a row, atomic UPSERT/DELETE)
 * Firestore path: organizations/{orgId}/trips/{docId}
 *
 * Fleetio → iCabbi Pipeline:
 *   1. Driver reports issue in Fleetio → webhook fires here
 *   2. Worker maps issue + site_ref to repair shop → creates iCabbi trip
 *   3. Writes notification to Firestore for the SMS platform UI
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
      if (url.pathname === "/webhook/fleetio" && request.method === "POST") {
        return await handleFleetioWebhook(request, env);
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

// ── Fleetio Webhook Handler ─────────────────────────────────────────────────
//
// Fleetio fires webhooks for issues/DVIRs. We:
//   1. Parse the issue details (vehicle, site/location, issue description)
//   2. Look up automation settings from Firestore (repair shop mappings, issue type mappings)
//   3. Create a trip in iCabbi (pickup at repair shop, name = "Issue / vehicle_ref", time = 23:59)
//   4. Write a notification to Firestore for the SMS platform UI

async function handleFleetioWebhook(request, env) {
  const signature = request.headers.get("x-fleetio-webhook-signature") || "";
  const expectedSecret = env.FLEETIO_WEBHOOK_SECRET || env.WEBHOOK_SECRET;

  if (!expectedSecret) {
    console.error("FLEETIO_WEBHOOK_SECRET not set");
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  // Read the raw body for signature verification
  const rawBody = await request.text();

  if (signature) {
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(expectedSecret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"]
    );
    const sig = await crypto.subtle.sign("HMAC", key, encoder.encode(rawBody));
    const computed = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, "0")).join("");

    console.log(`Fleetio HMAC: received="${signature.slice(0, 16)}..." computed="${computed.slice(0, 16)}..."`);

    if (computed !== signature) {
      return jsonResponse({ error: "Unauthorized" }, 401);
    }
  } else {
    const authHeader = request.headers.get("Authorization") || "";
    const rawSecret = authHeader.replace(/^(Bearer|Token)\s+/i, "").trim();
    if (!rawSecret || rawSecret !== expectedSecret) {
      console.log("No signature header and Authorization doesn't match");
      return jsonResponse({ error: "Unauthorized" }, 401);
    }
  }

  const payload = JSON.parse(rawBody);
  console.log("Fleetio webhook payload:", JSON.stringify(payload).slice(0, 3000));

  // Step 1: Parse the Fleetio issue to get driver name, vehicle, description
  const issue = normalizeFleetioIssue(payload);

  if (!issue || !issue.driverName) {
    console.log("Fleetio: no driver name found in payload, cannot match");
    await writeNotificationToFirestore(env, {
      type: "fleetio_issue",
      title: `Fleetio Issue: ${(issue?.description || "Unknown").slice(0, 60)}`,
      message: `No driver name found in Fleetio event. Cannot match to SMS contact. Vehicle: ${issue?.vehicleRef || "Unknown"}. Description: ${issue?.description || "N/A"}`,
      vehicleRef: issue?.vehicleRef || "",
      driverName: "",
      pipelineSteps: [
        { label: "Issue Reported", status: "done" },
        { label: "Driver Match", status: "pending" },
        { label: "Trip Creation", status: "pending" },
        { label: "Notification", status: "done" }
      ]
    });
    return jsonResponse({ ok: true, action: "notification_only", reason: "no_driver_name" });
  }

  console.log(`Fleetio issue: driver="${issue.driverName}" vehicle=${issue.vehicleRef} desc="${issue.description}"`);

  // Step 2: Look up driver in SMS tool contacts (Firestore threads) by name
  const contact = await findContactByName(env, issue.driverName);

  if (!contact) {
    console.log(`No SMS contact found matching driver "${issue.driverName}"`);
    await writeNotificationToFirestore(env, {
      type: "fleetio_issue",
      title: `Fleetio Issue: ${issue.description.slice(0, 60)}`,
      message: `Driver "${issue.driverName}" not found in SMS contacts. Vehicle: ${issue.vehicleRef || "Unknown"}. Description: ${issue.description}. Trip not auto-created.`,
      vehicleRef: issue.vehicleRef || "",
      driverName: issue.driverName,
      pipelineSteps: [
        { label: "Issue Reported", status: "done" },
        { label: "Driver Match", status: "pending" },
        { label: "Trip Creation", status: "pending" },
        { label: "Notification", status: "done" }
      ]
    });
    return jsonResponse({ ok: true, action: "notification_only", reason: "driver_not_found" });
  }

  console.log(`Matched driver "${issue.driverName}" → contact "${contact.name}" phone=${contact.phone}`);

  // Step 3: Look up vehicle in iCabbi to get site_ref for repair shop mapping
  //   Fleetio sends 3-digit ref (e.g. "197"), iCabbi uses 4-digit (e.g. "7197")
  const icabbiVehicleRef = toIcabbiVehicleRef(issue.vehicleRef);
  let siteRef = "";
  let vehicleId = null;

  if (icabbiVehicleRef && env.ICABBI_AUTH) {
    vehicleId = await lookupIcabbiVehicleId(env, icabbiVehicleRef);
    if (vehicleId) {
      siteRef = await lookupVehicleSiteRef(env, vehicleId) || "";
      console.log(`Vehicle ${icabbiVehicleRef} → id=${vehicleId} → site_ref="${siteRef}"`);
    } else {
      console.log(`Vehicle ${icabbiVehicleRef} not found in iCabbi`);
    }
  }

  // Step 4: Load automation settings and find repair shop by site_ref
  const settings = await loadAutomationSettings(env);
  const repairShop = findRepairShop(settings, siteRef, issue.description);
  const tripName = buildTripName(settings, issue);

  console.log(`Trip name: "${tripName}" | site_ref: "${siteRef}" | repair shop: "${repairShop?.shopName || "none"}"`);

  // Step 5: Create trip in iCabbi using their /bookings/add endpoint
  let icabbiResult = null;

  const pickupAddress = repairShop?.address || "Repair Shop (Address TBD)";
  const now = new Date();

  const icabbiBody = {
    date: now.toISOString(),
    name: tripName,
    phone: contact.phone || "0000000000",
    address: {
      formatted: pickupAddress
    },
    instructions: `Fleetio: ${issue.description}. Vehicle: ${issue.vehicleRef || "N/A"}. Driver: ${contact.name}.`
  };

  // If we found a site_ref, include it so iCabbi binds to the right site
  if (siteRef) {
    icabbiBody.site_ref = siteRef;
  }

  if (env.ICABBI_AUTH) {
    try {
      icabbiResult = await createIcabbiTrip(env, icabbiBody);
      console.log("iCabbi trip created:", JSON.stringify(icabbiResult));
    } catch (e) {
      console.error("iCabbi trip creation failed:", e.message);
      icabbiResult = { error: e.message };
    }
  } else {
    console.log("ICABBI_AUTH not configured — skipping trip creation. Body:", JSON.stringify(icabbiBody));
    icabbiResult = { skipped: true, reason: "icabbi_auth_not_configured" };
  }

  const tripCreated = icabbiResult && !icabbiResult.error && !icabbiResult.skipped;

  // Step 6: Store in Supabase as a local trip record
  const localTrip = {
    id: `fleetio_${issue.vehicleRef || "unknown"}_${Date.now()}`,
    date: now.toISOString().split("T")[0],
    time: now.toTimeString().slice(0, 5),
    vehicleRef: icabbiVehicleRef || issue.vehicleRef || "",
    pickup: pickupAddress,
    dropoff: "",
    passenger: tripName,
    passengerPhone: contact.phone || "",
    driverName: contact.name || issue.driverName,
    driverPhone: contact.phone || "",
    notes: icabbiBody.instructions,
    status: tripCreated ? "auto_created" : "pending_manual",
    receivedAt: now.toISOString()
  };
  await supabaseUpsertTrip(env, localTrip);

  // Step 7: Write notification to Firestore
  await writeNotificationToFirestore(env, {
    type: "trip_created",
    title: tripName,
    message: tripCreated
      ? `Auto-created trip for ${contact.name} (${contact.phone}). Vehicle: ${issue.vehicleRef} → site: ${siteRef || "unknown"}. Repair shop: ${repairShop?.shopName || "TBD"}.`
      : `Trip prepared for ${contact.name}. Vehicle: ${issue.vehicleRef}. iCabbi API ${icabbiResult?.skipped ? "not configured" : "error: " + icabbiResult?.error}. Saved locally.`,
    vehicleRef: issue.vehicleRef || "",
    siteRef: siteRef || "",
    driverName: contact.name || issue.driverName,
    driverPhone: contact.phone || "",
    tripName: tripName,
    repairShop: repairShop?.shopName || pickupAddress || "",
    pipelineSteps: [
      { label: "Issue Reported", status: "done" },
      { label: "Driver Match", status: "done" },
      { label: "Vehicle Lookup", status: vehicleId ? "done" : "pending" },
      { label: "Trip Creation", status: tripCreated ? "done" : "pending" },
      { label: "Notification", status: "done" }
    ]
  });

  return jsonResponse({
    ok: true,
    tripCreated,
    tripName,
    driverName: contact.name,
    driverPhone: contact.phone,
    vehicleRef: issue.vehicleRef
  });
}

// Look up a contact in SMS tool (Firestore threads) by driver name.
// Matches: exact name, last name + first initial, or case-insensitive contains.
async function findContactByName(env, driverName) {
  const token = await getFirebaseIdToken(env);
  if (!token) return null;

  const projectId = env.FIREBASE_PROJECT_ID || "sms-dlx";
  const colPath = `organizations/${ORG_ID}/threads`;
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${colPath}?key=${env.FIREBASE_API_KEY}&pageSize=500`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` }
  });

  if (!res.ok) {
    console.error("Failed to fetch threads for driver match:", await res.text());
    return null;
  }

  const data = await res.json();
  const documents = data.documents || [];

  const searchName = driverName.toLowerCase().trim();
  const searchParts = searchName.split(/\s+/);

  let bestMatch = null;

  for (const doc of documents) {
    const fields = doc.fields || {};
    const name = (fields.name?.stringValue || "").trim();
    const phone = (fields.phone?.stringValue || doc.name?.split("/").pop() || "").trim();
    const nameLower = name.toLowerCase();

    if (!name || !phone) continue;

    // Exact match
    if (nameLower === searchName) {
      return { name, phone };
    }

    // Last name match + first name starts with
    const contactParts = nameLower.split(/\s+/);
    if (searchParts.length >= 2 && contactParts.length >= 2) {
      const searchFirst = searchParts[0];
      const searchLast = searchParts[searchParts.length - 1];
      const contactFirst = contactParts[0];
      const contactLast = contactParts[contactParts.length - 1];

      if (searchLast === contactLast && (contactFirst.startsWith(searchFirst) || searchFirst.startsWith(contactFirst))) {
        bestMatch = { name, phone };
      }
    }

    // Contains match (either direction)
    if (!bestMatch && (nameLower.includes(searchName) || searchName.includes(nameLower))) {
      bestMatch = { name, phone };
    }
  }

  return bestMatch;
}

function normalizeFleetioIssue(raw) {
  const data = raw.payload || raw.data || raw;
  console.log(`normalizeFleetioIssue: keys=${Object.keys(data).slice(0,10).join(",")} name="${data.name}" summary="${data.summary}" desc="${data.description}"`);
  const reporter = data.reported_by || {};

  const vehicleRef = String(
    data.vehicle_name || data.vehicle_number || data.vehicle_ref ||
    data.vehicle_id || ""
  ).trim();

  const siteRef = String(
    reporter.group_name || data.group_name ||
    data.site_ref || data.location_name || ""
  ).trim();

  const description = String(
    data.name || data.summary || data.title ||
    data.description || ""
  ).trim();

  return {
    vehicleRef,
    siteRef,
    description,
    driverName: String(
      data.reported_by_name || reporter.name ||
      [reporter.first_name, reporter.last_name].filter(Boolean).join(" ") || ""
    ).trim(),
    driverPhone: String(reporter.mobile_phone_number || reporter.home_phone_number || "").trim(),
    driverEmail: String(reporter.email || raw.triggered_by || "").trim(),
    fleetioId: String(data.id || raw.id || "").trim(),
    fleetioNumber: String(data.number || "").trim(),
    eventType: String(raw.event || raw.event_type || "").trim()
  };
}

async function loadAutomationSettings(env) {
  try {
    const token = await getFirebaseIdToken(env);
    if (!token) return { repairMappings: [], issueMappings: [] };

    const projectId = env.FIREBASE_PROJECT_ID || "sms-dlx";
    const path = `organizations/${ORG_ID}/settings/automation`;
    const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${path}?key=${env.FIREBASE_API_KEY}`;

    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    if (!res.ok) {
      console.log("No automation settings found in Firestore, using env fallbacks");
      return buildSettingsFromEnv(env);
    }

    const doc = await res.json();
    const fields = doc.fields || {};

    const repairMappings = parseFirestoreArray(fields.repairMappings);
    const issueMappings = parseFirestoreArray(fields.issueMappings);

    return { repairMappings, issueMappings };
  } catch (e) {
    console.error("Failed to load automation settings:", e.message);
    return buildSettingsFromEnv(env);
  }
}

function parseFirestoreArray(field) {
  if (!field || !field.arrayValue || !field.arrayValue.values) return [];
  return field.arrayValue.values.map(v => {
    if (!v.mapValue || !v.mapValue.fields) return {};
    const f = v.mapValue.fields;
    const obj = {};
    for (const [key, val] of Object.entries(f)) {
      obj[key] = val.stringValue || val.integerValue || "";
    }
    return obj;
  });
}

// Fallback: build settings from environment variables
// Env format: REPAIR_MAPPINGS="RBG:Jiffy Lube:123 Main St Roseburg OR;EUG:Jiffy Lube:456 Oak Eugene OR"
function buildSettingsFromEnv(env) {
  const repairMappings = [];
  const issueMappings = [];

  if (env.REPAIR_MAPPINGS) {
    for (const entry of env.REPAIR_MAPPINGS.split(";")) {
      const [siteRef, shopName, address] = entry.split(":");
      if (siteRef) repairMappings.push({ siteRef: siteRef.trim(), shopName: (shopName || "").trim(), address: (address || "").trim() });
    }
  }

  if (env.ISSUE_MAPPINGS) {
    for (const entry of env.ISSUE_MAPPINGS.split(";")) {
      const [keyword, tripPrefix] = entry.split(":");
      if (keyword) issueMappings.push({ keyword: keyword.trim().toLowerCase(), tripPrefix: (tripPrefix || "").trim() });
    }
  }

  return { repairMappings, issueMappings };
}

function findRepairShop(settings, siteRef, description) {
  if (!settings.repairMappings || settings.repairMappings.length === 0) return null;

  const exactMatch = settings.repairMappings.find(
    m => m.siteRef && m.siteRef.toUpperCase() === (siteRef || "").toUpperCase()
  );
  if (exactMatch) return exactMatch;

  const defaultMapping = settings.repairMappings.find(
    m => m.siteRef === "*" || m.siteRef.toUpperCase() === "DEFAULT"
  );
  return defaultMapping || null;
}

// Trip name: "{issue description} / {3-digit vehicle number}" e.g. "Headlight / 197"
function buildTripName(settings, issue) {
  const vehicleShort = (issue.vehicleRef || "Unknown").replace(/^7/, ""); // strip leading 7 if 4-digit
  const desc = (issue.description || "").toLowerCase();

  if (settings.issueMappings && settings.issueMappings.length > 0) {
    for (const mapping of settings.issueMappings) {
      if (mapping.keyword && desc.includes(mapping.keyword)) {
        return `${mapping.tripPrefix} / ${vehicleShort}`;
      }
    }
  }

  const shortDesc = issue.description.length > 40
    ? issue.description.slice(0, 40) + "..."
    : issue.description;
  return `${shortDesc} / ${vehicleShort}`;
}

// Convert Fleetio's 3-digit vehicle ref to iCabbi's 4-digit (prefix with 7)
// e.g. "197" → "7197", "160" → "7160". If already 4+ digits, leave as-is.
function toIcabbiVehicleRef(fleetioRef) {
  const ref = String(fleetioRef || "").trim();
  if (!ref) return "";
  if (ref.length <= 3) return `7${ref}`;
  return ref;
}

// Look up vehicle ID in iCabbi by ref number
// GET {icabbi_url}/vehicle/id?ref={vehicle_ref}
async function lookupIcabbiVehicleId(env, vehicleRef) {
  const apiUrl = (env.ICABBI_API_URL || "https://api.icabbi.us/us4").replace(/\/+$/, "");
  const url = `${apiUrl}/vehicle/id?ref=${encodeURIComponent(vehicleRef)}`;

  console.log(`iCabbi vehicle lookup: ${url}`);
  const res = await fetch(url, {
    headers: { "Authorization": `Basic ${env.ICABBI_AUTH}` }
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`iCabbi vehicle/id lookup failed (${res.status}):`, errText);
    return null;
  }

  const data = await res.json();
  console.log("iCabbi vehicle/id response:", JSON.stringify(data).slice(0, 500));

  // Response may be { id: 1234 } or { body: { id: 1234 } } depending on API version
  const vehicleId = data.id || data.body?.id || data.vehicle_id || null;
  return vehicleId;
}

// Look up vehicle's site_ref from iCabbi
// GET {icabbi_url}/vehicle/sites?id={vehicle_id}
async function lookupVehicleSiteRef(env, vehicleId) {
  const apiUrl = (env.ICABBI_API_URL || "https://api.icabbi.us/us4").replace(/\/+$/, "");
  const url = `${apiUrl}/vehicle/sites?id=${encodeURIComponent(vehicleId)}`;

  console.log(`iCabbi vehicle/sites lookup: ${url}`);
  const res = await fetch(url, {
    headers: { "Authorization": `Basic ${env.ICABBI_AUTH}` }
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error(`iCabbi vehicle/sites lookup failed (${res.status}):`, errText);
    return null;
  }

  const data = await res.json();
  console.log("iCabbi vehicle/sites response:", JSON.stringify(data).slice(0, 500));

  // Extract site_ref from response — could be array of sites or single object
  if (Array.isArray(data)) {
    return data[0]?.site_ref || data[0]?.ref || data[0]?.name || null;
  }
  if (Array.isArray(data.body)) {
    return data.body[0]?.site_ref || data.body[0]?.ref || data.body[0]?.name || null;
  }
  return data.site_ref || data.ref || data.name || null;
}

// Create a trip in iCabbi via POST /bookings/add with Basic auth
async function createIcabbiTrip(env, tripBody) {
  const apiUrl = (env.ICABBI_API_URL || "https://api.icabbi.us/us4").replace(/\/+$/, "");
  const auth = env.ICABBI_AUTH; // Base64 Basic auth credentials

  const res = await fetch(`${apiUrl}/bookings/add`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Basic ${auth}`
    },
    body: JSON.stringify(tripBody)
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`iCabbi API ${res.status}: ${errText}`);
  }

  return await res.json();
}

async function writeNotificationToFirestore(env, data) {
  const token = await getFirebaseIdToken(env);
  if (!token) {
    console.warn("No Firebase token — cannot write notification");
    return;
  }

  const projectId = env.FIREBASE_PROJECT_ID || "sms-dlx";
  const colPath = `organizations/${ORG_ID}/notifications`;
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${colPath}?key=${env.FIREBASE_API_KEY}`;

  const fields = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === null || value === undefined) {
      fields[key] = { nullValue: null };
    } else if (Array.isArray(value)) {
      fields[key] = {
        arrayValue: {
          values: value.map(item => {
            if (typeof item === "object") {
              const mapFields = {};
              for (const [k, v] of Object.entries(item)) {
                mapFields[k] = { stringValue: String(v) };
              }
              return { mapValue: { fields: mapFields } };
            }
            return { stringValue: String(item) };
          })
        }
      };
    } else if (typeof value === "boolean") {
      fields[key] = { booleanValue: value };
    } else {
      fields[key] = { stringValue: String(value) };
    }
  }

  fields.read = { booleanValue: false };
  fields.createdAt = { timestampValue: new Date().toISOString() };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ fields }),
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error("Notification write failed:", errText);
  } else {
    console.log("Notification written to Firestore");
  }
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
