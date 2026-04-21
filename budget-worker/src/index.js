import { createClient } from './supabase.js';

// ─── Helpers ─────────────────────────────────────────────────

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

async function findAccount(db, accountNumber, accountName) {
  const num = String(accountNumber || '').trim();
  const name = String(accountName || '').trim();

  if (num) {
    let rows = await db.query('accounts', { filters: { ref: num } });
    if (rows.length) return rows[0];

    rows = await db.query('accounts', { filters: { icabbi_id: num } });
    if (rows.length) return rows[0];
  }

  if (name) {
    const rows = await db.query('accounts', { filters: { name: name.toUpperCase() } });
    if (rows.length) return rows[0];
  }

  return null;
}

// ─── NORMALIZE ───────────────────────────────────────────────

function normalizeIcabbiBooking(body) {
  return {
    booking_id: body.booking_id,
    account_ref: body.account_ref,
    account_name: body.account_name,
    estimated_fare: parseFloat(
      body.booking_total_price || body.booking_price || 0
    )
  };
}

// ─── BOOKING LOGIC (PRE-TRIP GUARD) ──────────────────────────

async function processBooking(db, payload, env) {
  const { booking_id, account_ref, account_name, estimated_fare } =
    normalizeIcabbiBooking(payload);

  if (!account_ref) {
    return { statusCode: 400, body: { error: 'Missing account_ref' } };
  }

  const bookingValue = parseFloat(estimated_fare || 0);

  const account = await findAccount(db, account_ref, account_name);

  if (!account) {
    return {
      statusCode: 200,
      body: { status: 'ignored', reason: 'account_not_found' }
    };
  }

  const month = new Date().toISOString().slice(0, 7) + '-01';

  const rows = await db.query('budget_months', {
    filters: {
      account_ref: account.ref,
      month_label: month
    }
  });

  if (!rows.length) {
    return {
      statusCode: 200,
      body: { status: 'no_budget', account_ref: account.ref }
    };
  }

  const budget = rows[0];
  const limit = parseFloat(budget.monthly_limit || 0);
  const used = parseFloat(budget.used_amount || 0);

  const remaining = limit - used;
  const willExceed = bookingValue > remaining;

  if (remaining <= 0 || willExceed) {
    console.log(`[booking] CANCEL ${booking_id} → ${account.ref}`);

    // BUG FIX: iCabbi API is POST /bookings/cancel/{trip_id}, not /bookings/{id}/cancel
    await cancelBooking(env, booking_id);

    return {
      statusCode: 200,
      body: {
        status: 'cancelled',
        account_ref: account.ref,
        remaining
      }
    };
  }

  console.log(`[booking] ALLOW ${booking_id} → ${account.ref}`);

  return {
    statusCode: 200,
    body: { status: 'allowed', remaining }
  };
}

// ─── COMPLETED TRIP LOGGING ──────────────────────────────────

async function processSingleTrip(db, payload) {
  const {
    booking_id,
    account_number,
    account_name,
    date,
    fare
  } = payload || {};

  if (!account_number) {
    return { statusCode: 400, body: { error: 'Missing account_number' } };
  }

  const account = await findAccount(db, account_number, account_name);

  if (!account) {
    return {
      statusCode: 200,
      body: { status: 'dropped', reason: 'account_not_matched' }
    };
  }

  const tripDate = new Date(date);
  const formattedDate = tripDate.toISOString().split('T')[0];
  const fareNum = parseFloat(fare) || 0;

  await db.insert('trips', {
    booking_id: String(booking_id),
    account_ref: account.ref,
    account_name: account.name,
    account_group: account.account_group,
    fare: fareNum,
    trip_date: formattedDate,
    raw_payload: payload
  });

  return {
    statusCode: 200,
    body: { status: 'ok' }
  };
}

// ─── HANDLERS ────────────────────────────────────────────────

async function handleSingleTrip(db, request) {
  const payload = await request.json();
  const result = await processSingleTrip(db, payload);
  return json(result.body, result.statusCode);
}

async function handleBatchTrips(db, request) {
  const body = await request.json();
  const trips = body?.trips || [];

  const results = [];

  for (const trip of trips) {
    try {
      const result = await processSingleTrip(db, trip);
      results.push(result.body);
    } catch (err) {
      results.push({ status: 'error', error: err.message });
    }
  }

  return json({ results });
}

async function handleStats(db) {
  const accounts = await db.query('accounts', { select: 'account_group' });
  return json({ total_accounts: accounts.length });
}

function handleHealth() {
  return json({ status: 'ok' });
}

// ─── ACCOUNT VALIDATION ───────────────────────────────────────

async function handleAccountValidation(db, request) {
  try {
    const payload = await request.json();
    const { account_number, account_name, estimated_fare = 0 } = payload;

    if (!account_number) {
      return json({ valid: false, message: 'Missing account number' }, 400);
    }

    // BUG FIX: was missing account_name as third argument
    const account = await findAccount(db, account_number, account_name);

    if (!account) {
      return json({ valid: false, message: 'Account not found or inactive' });
    }

    if (!account.active) {
      return json({ valid: false, message: 'Account is deactivated' });
    }

    const summaryRows = await db.query('monthly_account_summary', {
      filters: { account_ref: account.ref }
    });

    if (!summaryRows || summaryRows.length === 0) {
      return json({ valid: true, message: 'Account valid (No usage data)' });
    }

    const summary = summaryRows[0];
    const remaining = parseFloat(summary.remaining_budget);
    const estFare = parseFloat(estimated_fare) || 0;

    if (remaining <= 0) {
      return json({
        valid: false,
        message: `Account suspended: Budget exceeded. Remaining: $${remaining.toFixed(2)}`
      });
    }

    if (remaining < estFare) {
      return json({
        valid: false,
        message: `Insufficient funds. Estimated fare: $${estFare.toFixed(2)}, Remaining: $${remaining.toFixed(2)}`
      });
    }

    return json({
      valid: true,
      message: 'Account valid',
      remaining_budget: remaining
    });

  } catch (err) {
    console.error('[validation] Error:', err.message);
    return json({ valid: false, message: 'Internal validation error' }, 500);
  }
}

// ─── CANCEL ──────────────────────────────────────────────────

async function cancelBooking(env, booking_id) {
  // iCabbi API: POST /bookings/cancel/{trip_id}
  const apiBase = (env.ICABBI_API || '').replace(/\/+$/, '');
  const res = await fetch(`${apiBase}/bookings/cancel/${booking_id}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.ICABBI_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  if (!res.ok) {
    console.error(`[cancel] iCabbi cancel failed for ${booking_id}: ${await res.text()}`);
  }
  return res;
}

// ─── MAIN ────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method;

    if (method === 'GET' && pathname === '/health') {
      return handleHealth();
    }

    // All /webhook/* routes require the shared secret
    if (method === 'POST' && pathname.startsWith('/webhook/')) {
      const secret = request.headers.get('x-webhook-secret');

      // BUG FIX: /webhook/validate was outside this block and had no auth check
      if (!env.WEBHOOK_SECRET || secret !== env.WEBHOOK_SECRET) {
        return json({ error: 'Unauthorized' }, 401);
      }

      try {
        // FAST ROUTE: instantly queue — no DB connection needed
        if (pathname === '/webhook/icabbi/booking') {
          const payload = await request.json();
          await env.BOOKING_QUEUE.send({
            payload,
            received_at: new Date().toISOString(),
          });
          return json({ status: 'queued' }, 202);
        }

        // SYNCHRONOUS ROUTES: establish DB connection only when needed
        const db = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);

        if (pathname === '/webhook/validate') {
          return await handleAccountValidation(db, request);
        }

        if (pathname === '/webhook/icabbi') {
          return await handleSingleTrip(db, request);
        }

        if (pathname === '/webhook/icabbi/batch') {
          return await handleBatchTrips(db, request);
        }

      } catch (err) {
        console.error('[webhook] Error:', err.message);
        return json({ error: 'Internal server error' }, 500);
      }
    }

    if (method === 'GET' && pathname === '/webhook/stats') {
      const db = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
      return handleStats(db);
    }

    return json({ error: 'Not found' }, 404);
  },

  // ─── QUEUE CONSUMER ───────────────────────────────────────
  // Cloudflare delivers messages in batches. One DB connection serves the whole batch.
  // Retry on 5xx (transient errors); ack everything else (including business rejections).
  async queue(batch, env) {
    const db = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);

    for (const msg of batch.messages) {
      try {
        const { payload } = msg.body;
        const result = await processBooking(db, payload, env);

        if (result.statusCode >= 500) {
          msg.retry();
        } else {
          msg.ack();
        }
      } catch (err) {
        console.error('[queue] Failed booking:', err.message);
        msg.retry();
      }
    }
  },
};
