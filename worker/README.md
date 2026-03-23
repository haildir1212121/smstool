# iCabbi Trip Webhook Worker

Cloudflare Worker that receives iCabbi "Pre-booking: Driver Designated" webhooks and stores trip data for the SMS tool frontend.

## Setup

1. Install wrangler CLI: `npm install -g wrangler`
2. Login: `wrangler login`
3. Create KV namespace:
   ```bash
   wrangler kv namespace create TRIPS
   ```
4. Copy the namespace ID into `wrangler.toml`
5. Set the webhook secret:
   ```bash
   wrangler secret put WEBHOOK_SECRET
   # Enter a strong random string (this is shared with iCabbi + frontend)
   ```
6. Deploy:
   ```bash
   cd worker
   npm install
   npm run deploy
   ```

## iCabbi Webhook Configuration

In iCabbi, configure the "Pre-booking: Driver Designated" event to POST to:
```
https://icabbi-trip-webhook.<your-subdomain>.workers.dev/webhook
```

Set the custom header:
```
X-Webhook-Secret: <your-secret>
```

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/webhook` | `X-Webhook-Secret` header | Receives iCabbi webhook events |
| GET | `/trips?from=YYYY-MM-DD&to=YYYY-MM-DD` | `Authorization: Bearer <secret>` | Fetch stored trips for date range |
| GET | `/health` | None | Health check |

## How It Works

1. iCabbi fires "Pre-booking: Driver Designated" → POST arrives at `/webhook`
2. Worker extracts trip date, normalizes fields, upserts into KV by trip ID
3. KV key format: `trips:YYYY-MM-DD` → array of trip objects
4. KV entries auto-expire after 7 days
5. Frontend fetches `/trips?from=...&to=...` to get all trips for the date range
6. Frontend filters by vehicle ref (from uploaded spreadsheet) and generates SMS messages

## Frontend Settings

In the SMS tool, go to **Webhook Settings** and enter:
- **Worker URL**: `https://icabbi-trip-webhook.<your-subdomain>.workers.dev`
- **Webhook Secret**: same secret you set in step 5
