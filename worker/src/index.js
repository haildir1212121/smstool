/**
 * iCabbi Trips Webhook Worker
 *
 * POST /webhook              — receives iCabbi "Pre-booking: Driver Designated" events
 * POST /webhook/undesignate  — receives iCabbi "Driver Undesignate" events (deletes trip)
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
  // Fleetio sends the secret via the "Authorization" HTTP header,
  // potentially prefixed with "Token ", "Bearer ", or sent raw
  const authHeader = request.headers.get("Authorization") || "";
  const xSecret = request.headers.get("X-Webhook-Secret") || "";
  const rawSecret = authHeader.replace(/^(Bearer|Token)\s+/i, "").trim() || xSecret;
  const expectedSecret = env.FLEETIO_WEBHOOK_SECRET || env.WEBHOOK_SECRET;

  console.log(`Fleetio auth: header="${authHeader.slice(0, 20)}..." expected="${expectedSecret ? "set" : "NOT SET"}"`);

  if (!expectedSecret || rawSecret !== expectedSecret) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  const payload = await request.json();
  console.log("Fleetio webhook payload:", JSON.stringify(payload).slice(0, 3000));

  // Fleetio can send various event types. We care about issues/DVIRs.
  // Normalize the payload — Fleetio sends different shapes for different events
  const issue = normalizeFleetioIssue(payload);

  if (!issue || !issue.description) {
    return jsonResponse({ error: "No actionable issue found in payload" }, 400);
  }

  console.log(`Fleetio issue: vehicle=${issue.vehicleRef} site=${issue.siteRef} desc="${issue.description}"`);

  // Step 1: Load automation settings from Firestore
  const settings = await loadAutomationSettings(env);

  // Step 2: Find repair shop mapping for this site_ref
  const repairShop = findRepairShop(settings, issue.siteRef, issue.description);

  if (!repairShop) {
    console.log(`No repair shop mapping found for site_ref="${issue.siteRef}". Writing notification only.`);
    // Still write a notification even without a trip
    await writeNotificationToFirestore(env, {
      type: "fleetio_issue",
      title: `Fleetio Issue: ${issue.description.slice(0, 60)}`,
      message: `Vehicle ${issue.vehicleRef || "Unknown"} at ${issue.siteRef || "Unknown site"} reported: ${issue.description}. No repair shop mapping found — trip not auto-created.`,
      vehicleRef: issue.vehicleRef || "",
      siteRef: issue.siteRef || "",
      driverName: issue.driverName || "",
      pipelineSteps: [
        { label: "Issue Reported", status: "done" },
        { label: "Trip Creation", status: "pending" },
        { label: "Notification", status: "done" }
      ]
    });
    return jsonResponse({ ok: true, action: "notification_only", reason: "no_repair_mapping" });
  }

  // Step 3: Determine trip name from issue type mapping
  const tripName = buildTripName(settings, issue);

  // Step 4: Create trip in iCabbi
  let icabbiResult = null;
  const icabbiApiUrl = env.ICABBI_API_URL || "";
  const icabbiApiKey = env.ICABBI_API_KEY || "";

  const tripData = {
    pickup_address: repairShop.address,
    pickup_name: repairShop.shopName || repairShop.address,
    passenger_name: tripName,
    vehicle_ref: issue.vehicleRef || "",
    site_ref: issue.siteRef || "",
    date: new Date().toISOString().split("T")[0],
    time: "23:59",
    notes: `Auto-created from Fleetio issue: ${issue.description}`
  };

  if (icabbiApiUrl && icabbiApiKey) {
    try {
      icabbiResult = await createIcabbiTrip(icabbiApiUrl, icabbiApiKey, tripData);
      console.log("iCabbi trip created:", JSON.stringify(icabbiResult));
    } catch (e) {
      console.error("iCabbi trip creation failed:", e.message);
      icabbiResult = { error: e.message };
    }
  } else {
    console.log("iCabbi API not configured — skipping trip creation. Trip data:", JSON.stringify(tripData));
    icabbiResult = { skipped: true, reason: "icabbi_api_not_configured" };
  }

  const tripCreated = icabbiResult && !icabbiResult.error && !icabbiResult.skipped;

  // Step 5: Also store in Supabase as a local trip record
  const localTrip = {
    id: `fleetio_${issue.vehicleRef || "unknown"}_${Date.now()}`,
    date: tripData.date,
    time: tripData.time,
    vehicleRef: tripData.vehicle_ref,
    pickup: tripData.pickup_address,
    dropoff: "",
    passenger: tripData.passenger_name,
    passengerPhone: "",
    driverName: issue.driverName || "",
    driverPhone: issue.driverPhone || "",
    notes: tripData.notes,
    status: tripCreated ? "auto_created" : "pending_manual",
    receivedAt: new Date().toISOString()
  };
  await supabaseUpsertTrip(env, localTrip);

  // Step 6: Write notification to Firestore
  await writeNotificationToFirestore(env, {
    type: "trip_created",
    title: tripName,
    message: tripCreated
      ? `Auto-created trip for vehicle ${issue.vehicleRef} → ${repairShop.shopName || repairShop.address}. Pickup at 23:59.`
      : `Trip prepared for vehicle ${issue.vehicleRef} → ${repairShop.shopName || repairShop.address}. iCabbi API ${icabbiResult?.skipped ? "not configured" : "error: " + icabbiResult?.error}. Saved locally.`,
    vehicleRef: issue.vehicleRef || "",
    siteRef: issue.siteRef || "",
    tripName: tripName,
    repairShop: repairShop.shopName || repairShop.address,
    driverName: issue.driverName || "",
    pipelineSteps: [
      { label: "Issue Reported", status: "done" },
      { label: "Trip Creation", status: tripCreated ? "done" : "pending" },
      { label: "Notification", status: "done" }
    ]
  });

  return jsonResponse({
    ok: true,
    tripCreated,
    tripName,
    repairShop: repairShop.shopName || repairShop.address,
    vehicleRef: issue.vehicleRef
  });
}

// Parse Fleetio webhook payload into a normalized issue object
function normalizeFleetioIssue(payload) {
  // Fleetio webhooks can have various structures depending on the event type
  // Common: { event_type, data: { ... } } or direct issue object
  const data = payload.data || payload;
  const vehicle = data.vehicle || {};
  const submitter = data.submitted_by || data.driver || data.user || {};

  // Extract vehicle ref — Fleetio uses vehicle number, name, or custom ref
  const vehicleRef = String(
    vehicle.number || vehicle.name || vehicle.ref ||
    data.vehicle_number || data.vehicle_name || data.vehicle_ref ||
    data.vehicle_id || ""
  ).trim();

  // Extract site/location — maps to iCabbi site_ref
  const siteRef = String(
    data.site_ref || data.location_name || data.group_name ||
    vehicle.group_name || vehicle.location || data.site || ""
  ).trim();

  // Issue description — from DVIR items, issue body, or comment
  let description = "";
  if (data.dvir_items && Array.isArray(data.dvir_items)) {
    description = data.dvir_items
      .filter(item => item.status === "failed" || item.condition === "failed" || item.defect)
      .map(item => item.name || item.label || item.description || "Unknown item")
      .join(", ");
  }
  if (!description) {
    description = data.description || data.title || data.summary ||
      data.issue_description || data.comment || data.body || "";
  }

  return {
    vehicleRef,
    siteRef,
    description: String(description).trim(),
    driverName: [submitter.first_name, submitter.last_name].filter(Boolean).join(" ") ||
      submitter.name || data.driver_name || "",
    driverPhone: submitter.phone || data.driver_phone || "",
    fleetioId: String(data.id || data.issue_id || data.dvir_id || payload.id || "").trim(),
    eventType: payload.event_type || payload.type || ""
  };
}

// Load automation settings (repair mappings, issue mappings) from Firestore
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

    // Parse array fields from Firestore format
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

// Find the repair shop for a given site_ref
function findRepairShop(settings, siteRef, description) {
  if (!settings.repairMappings || settings.repairMappings.length === 0) return null;

  // First try exact site_ref match
  const exactMatch = settings.repairMappings.find(
    m => m.siteRef && m.siteRef.toUpperCase() === (siteRef || "").toUpperCase()
  );
  if (exactMatch) return exactMatch;

  // Fall back to a default mapping (siteRef = "*" or "DEFAULT")
  const defaultMapping = settings.repairMappings.find(
    m => m.siteRef === "*" || m.siteRef.toUpperCase() === "DEFAULT"
  );
  return defaultMapping || null;
}

// Build the trip name from issue mappings: "Headlight repair / VEH123"
function buildTripName(settings, issue) {
  const desc = (issue.description || "").toLowerCase();

  // Check issue mappings for a matching keyword
  if (settings.issueMappings && settings.issueMappings.length > 0) {
    for (const mapping of settings.issueMappings) {
      if (mapping.keyword && desc.includes(mapping.keyword)) {
        return `${mapping.tripPrefix} / ${issue.vehicleRef || "Unknown"}`;
      }
    }
  }

  // Fallback: use the raw description (truncated) as the trip name
  const shortDesc = issue.description.length > 40
    ? issue.description.slice(0, 40) + "..."
    : issue.description;
  return `${shortDesc} / ${issue.vehicleRef || "Unknown"}`;
}

// Create a trip in iCabbi via their API
async function createIcabbiTrip(apiUrl, apiKey, tripData) {
  const url = `${apiUrl.replace(/\/+$/, "")}/bookings`;

  const body = {
    pickup_address: tripData.pickup_address,
    pickup_name: tripData.pickup_name,
    passenger_name: tripData.passenger_name,
    vehicle_ref: tripData.vehicle_ref,
    date: tripData.date,
    time: tripData.time,
    notes: tripData.notes,
    status: "pre_booked"
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`,
      "X-API-Key": apiKey
    },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`iCabbi API ${res.status}: ${errText}`);
  }

  return await res.json();
}

// Write a notification document to Firestore for the SMS platform UI
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

  // Add read=false and createdAt timestamp
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
