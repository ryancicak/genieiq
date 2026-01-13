# GenieIQ - 2 Minute Setup (Local + Lakebase)

This guide is intentionally short and copy/paste friendly. You should be able to go from **zero → running** in ~2 minutes.

## What you’ll do

- Create/update your local `.env` using the built-in setup helper
- (Optional but recommended) Paste a **Lakebase OAuth token** so history/starred/new-spaces persist locally
- Start GenieIQ

## 1) Run the setup helper

From the repo root:

```bash
./setup.sh
```

macOS (Finder): double‑click `scripts/Setup_GenieIQ.command`.

The script writes to `.env` (which is **gitignored**).

## If you are deploying GenieIQ as a Databricks App (first-time deploy)

For customer-style installs (not local dev), the fastest path is:

```bash
./deploy.sh
```

`./deploy.sh` will:

- Build the frontend
- Upload the runtime bundle to the Workspace
- Deploy the Databricks App
- Try to **create/connect** a Lakebase instance (`genieiq-db`) so history persists

If Lakebase isn’t enabled (or you don’t have permissions), GenieIQ still deploys and runs, but history will be in-memory until Lakebase is restored.

## 2) Get the Lakebase OAuth token (PGPASSWORD)

This is the key step that makes local Lakebase persistence “just work”.

### In Databricks

- Open your workspace in the browser
- Go to **Compute → Database instances**
- Click your Lakebase instance (example: `genieiq-db`)
- Open the **Credentials** section
- Click **Get OAuth token (1 hr lifetime) for PGPASSWORD**

Now paste that token when `setup.sh` asks for `LAKEBASE_TOKEN`.

### Notes

- **This token expires in ~1 hour**. If Lakebase suddenly stops working, you almost always just need a fresh token.
- The token is used as the **Postgres password**. Your Postgres username is typically **your Databricks user email**.

## 3) Start GenieIQ

```bash
npm run install:all
npm run dev
```

You should see:

- Frontend: `http://localhost:5173`
- API: `http://localhost:3001`

## Troubleshooting (fast)

- **Lakebase shows “Invalid authorization …”**
  - Your `LAKEBASE_TOKEN` is missing/expired.
  - Get a fresh token from the Lakebase UI and re-run `./setup.sh` (or paste it into `.env`).

- **Spaces load, but history/starred/new don’t persist**
  - Lakebase isn’t connected → check `LAKEBASE_HOST`, `LAKEBASE_DATABASE`, `DATABRICKS_USER`, `LAKEBASE_TOKEN`.

- **Not sure what to put for LAKEBASE_HOST / LAKEBASE_DATABASE**
  - In the Lakebase instance UI, copy the `psql` connection string.
  - `host=...` → `LAKEBASE_HOST`
  - `dbname=...` → `LAKEBASE_DATABASE`

## What gets stored in Lakebase

GenieIQ auto-creates/migrates schema at runtime (idempotent) in `backend/services/lakebase.js`:

- `audit_results`: one row per scan (timestamps + scores)
- `latest_scores`: fast view for “Scored”
- `space_stars`: per-user starred spaces
- `spaces_seen`: first-seen/last-seen registry powering “New spaces”

For manual setup/review: `sql/setup_lakebase.sql`.

