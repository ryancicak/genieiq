# GenieIQ

**Boost your Genie Space intelligence.**

GenieIQ analyzes your Databricks Genie spaces and provides actionable recommendations to improve answer quality. No more reading 47-page documentation. Just connect your space, see your score, and follow the steps.

## Features

- **GenieIQ Score (0-100)**: Instant assessment of your Genie space setup
- **Maturity Levels**: Emerging to Developing to Maturing to Optimized
- **Actionable Recommendations**: Specific steps with point values
- **Admin Dashboard**: Org-wide visibility for workspace admins
- **Historical Tracking**: See improvement over time
- **Starred Spaces**: Curate your important spaces with a per-user star
- **New Spaces**: Recently discovered feed and filter (fast and optional)

## Quick Start

### Prerequisites

- Node.js 18+
- Databricks workspace with Genie enabled
- Personal Access Token (for local development)

### One-pass Local Setup (recommended)

Run the setup helper and follow the prompts (creates/updates `.env`):

```bash
./setup.sh
```

macOS (Finder): double-click `scripts/Setup_GenieIQ.command`.

**Lakebase (local) token note**: Lakebase Postgres typically needs an **OAuth token** (1 hour lifetime) as the password, not a PAT. The setup script will ask you to paste it into `LAKEBASE_TOKEN` (stored only in your local `.env`, which is gitignored).

Full walkthrough (2 minutes): see `setup.md`.

### Local Development

```bash
# Clone and install
cd genieiq
npm run install:all

# Configure environment
cp env.example .env
# Edit .env with your Databricks credentials

# Start development servers
npm run dev
```

The app will be available at `http://localhost:5173` (frontend) with the API at `http://localhost:3001`.

### Deploy to Databricks Apps

```bash
# Fully automated deploy (builds frontend, bundles runtime assets, uploads to Workspace, deploys the app)
./deploy.sh
```

Notes:
- The deploy bundle is saved under `./.tmp/deploy-bundles/` so **everything stays inside this repo folder**.
- The deploy bundle uses `package-deploy.json` (copied as `package.json`) to avoid Databricks Apps trying to run a frontend build at deploy time.

### Deploy to a Different Workspace (easy)

- **Option A (recommended)**: authenticate the Databricks CLI to the target workspace, then run `./deploy.sh`.

```bash
# If you use profiles, create/use one for the target workspace:
databricks auth login --host "https://<your-workspace>" --profile target
export DATABRICKS_CONFIG_PROFILE=target

./deploy.sh
```

- **Option B (macOS double-click)**: run `scripts/Deploy_GenieIQ.command` from Finder.

#### Lakebase notes

- If the target workspace has **Lakebase Provisioned** enabled and you have permissions, `./deploy.sh` will create/use a Lakebase instance automatically.
- If Lakebase isn’t available, GenieIQ still deploys and runs (it falls back to **in-memory** storage).
- To force skipping Lakebase:

```bash
export SKIP_LAKEBASE=1
./deploy.sh
```

#### Lakebase schema (important)

- GenieIQ **auto-initializes and auto-migrates** its Lakebase schema at runtime in `backend/services/lakebase.js` (idempotent).
- This includes:
  - `audit_results` (one row per scan, with timestamps)
  - `latest_scores` view (fast scored list)
  - `space_stars` (per-user starred spaces)
  - `spaces_seen` (first-seen/last-seen registry powering “New spaces”)
- For manual setup / review, see `sql/setup_lakebase.sql` (kept in this repo for portability).

### One-click Local Backup (macOS)

- Double-click `scripts/Backup_GenieIQ.command` to create a timestamped backup archive inside `./backups/`.

## Scoring Rubric

GenieIQ scores are based on Genie space best practices:

| Category | Max Points | What's Measured |
|----------|------------|-----------------|
| Foundation | 30 | Dedicated warehouse, text instructions |
| Data Setup | 25 | Joins, sample questions, table docs |
| SQL Assets | 25 | SQL expressions, trusted queries |
| Optimization | 20 | Feedback collection, iteration |

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Databricks App                      │
├─────────────────────────────────────────────────┤
│  Node.js/Express Backend                         │
│  ├── Databricks API integration                  │
│  ├── Scoring engine                              │
│  └── REST API                                    │
├─────────────────────────────────────────────────┤
│  React Frontend (Vite)                           │
│  ├── Owner view (my spaces)                      │
│  └── Admin view (all spaces)                     │
└─────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────┐
│  Databricks Services                             │
│  ├── Genie Spaces API                            │
│  ├── SQL Warehouses API                          │
│  ├── Unity Catalog API                           │
│  ├── Databricks Assistant (agent in fix notebooks)│
│  └── Lakebase Provisioned (PostgreSQL storage)   │
└─────────────────────────────────────────────────┘
```

## License

MIT

