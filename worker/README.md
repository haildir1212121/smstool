# iCabbi Trips Webhook Worker

Cloudflare Worker that receives iCabbi webhook events and serves trip data to the frontend.

## Setup

1. Install dependencies:
   ```bash
   cd worker
   npm install
   ```

2. Create a KV namespace:
   ```bash
   npx wrangler kv namespace create TRIPS
   ```
   Copy the output `id` into `wrangler.toml`.

3. Set the webhook secret:
   ```bash
   npx wrangler secret put WEBHOOK_SECRET
   ```
   Enter a strong random string. You'll configure the same secret in:
   - iCabbi webhook config (as the `X-Webhook-Secret` header)
   - The frontend settings (so it displays correctly)

4. Deploy:
   ```bash
   npm run deploy
   ```

## Endpoints

### `POST /webhook`
Receives iCabbi "Pre-booking: Driver Designated" events.

**Headers:**
- `X-Webhook-Secret: <your-secret>` (required)

**Body:** iCabbi booking JSON (single object or array).

### `GET /trips?from=YYYY-MM-DD&to=YYYY-MM-DD`
Returns all stored trips for the date range, grouped by date.

### `GET /trips/stats?from=YYYY-MM-DD&to=YYYY-MM-DD`
Returns trip counts per date.

## KV Structure

- Key: `trips:2026-03-24` → Value: JSON array of normalized trip objects
- Auto-expires after 7 days
- Each webhook POST upserts by trip ID (no duplicates)

## iCabbi Webhook Configuration

In your iCabbi admin panel, configure a webhook for "Pre-booking: Driver Designated":
- URL: `https://your-worker.workers.dev/webhook`
- Method: POST
- Header: `X-Webhook-Secret: <your-secret>`
