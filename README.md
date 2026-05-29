# Brickstar Hotel Daily Performance · Sample Validation Project

A reference **Databricks Asset Bundle** that wires up the recommended target
architecture — managed CDC + custom HTTP fan-out + Lakeflow SDP medallion +
governed Metric View + AI/BI dashboard + Genie Space — end-to-end on a real
workspace. Both data sources are mocked so the project deploys without any
external dependency; swapping in real sources is a 2-task substitution
(see [`docs/Architecture.md`](docs/Architecture.md#10--from-sample-to-production--migration-checklist)).

## The use case it solves

A daily portfolio-performance board for a hotel chain — the kind of dashboard
a revenue manager opens with their morning coffee.

> **Business question:** "How are my hotels performing today, where am I losing
> money, and which segments are pulling the portfolio?"

### Data shape

- **30 hotels** across 15 destinations (Mallorca, Cancún, Punta Cana, Algarve, …) and 5 market segments (Beachfront Luxury · All-Inclusive · Family Resort · Urban · Wellness)
- **60 days of history** generated synthetically
- ~**1,800 Gold rows** (hotel × day) and **~1,800 JSON payloads** in the RAW Volume

### KPIs delivered via the Metric View

| KPI | How it is computed |
|---|---|
| **Total revenue** | `SUM(daily_revenue_eur)` |
| **RevPAR** | `revenue / rooms_available` per hotel-day, then averaged |
| **ADR** | `AVG(adr_eur)` |
| **Occupancy %** | `rooms_occupied / rooms_total` |
| **NPS** | `AVG(nps_score)` |
| **GOP proxy** | `revenue × segment-specific margin (46-62%)` |
| **Cancellations · walk-ins** | Operational volumes |

### What it demonstrates technically

The business case is hotels, but the pattern is generic to any multi-source
daily-KPI use case:

| Pattern | How this sample exposes it |
|---|---|
| Managed ingest (Lakeflow Connect SQL Server CDC) | Tables `hotels` + `reservations` written with audit columns |
| Custom ingest (HTTP fan-out) | Notebook reads a control table, fires N calls per hotel-day, writes JSON to a UC Volume partitioned by `hotel_id/year/month/day/` |
| Lakebase-style parametrisation | `control_endpoints` Delta table (Lakebase-equivalent semantics) |
| RAW Volume → Auto Loader → Bronze | `bronze_web_metrics` streaming via `cloudFiles` |
| Silver with Expectations DQ | `@dlt.expect_or_drop("valid_dates")`, `@dlt.expect_or_fail("hotel_id_not_null")`, dedup, status normalisation |
| Gold + UC Metric View | One KPI definition consumed by every engine via `MEASURE(...)` |
| AI/BI dashboard on the Metric View | Counters + trend lines + top-N bar + segment table, Brickstar-styled |
| Genie Space (NL → SQL, governed) | "Top 5 hotels by RevPAR this week" → SQL generated automatically against the Metric View |
| Liquid Clustering + Predictive Optimization | `cluster_by=[hotel_id, metric_date]` across all SDP tables; auto OPTIMIZE/VACUUM |

Once you replace `00_seed_mock_sources.py` (with real Lakeflow Connect for SQL
Server) and `01_mock_http_fanout.py` (with your real HTTP service +
`httpx.AsyncClient`), **the rest of the pipeline runs unchanged** — Silver and
Gold consume the same table names.

## What gets deployed

| Component | Type | Notes |
|---|---|---|
| `brickstar_validation` | UC schema | Created · holds everything |
| `raw_http` | UC managed volume | JSON landing zone |
| `Brickstar Serverless Warehouse` | SQL warehouse | Serverless PRO Medium · Photon · auto-stop 10 min |
| `hotels`, `reservations`, `control_endpoints` | Delta tables | Seeded by Notebook 00 (mocks Lakeflow Connect SQL Server + Lakebase control table) |
| `brickstar_medallion` | Lakeflow SDP serverless pipeline | Auto Loader → Bronze → Silver (DQ expectations) → Gold |
| `gold_hotel_daily_kpis` | Materialized view | RevPAR · ADR · occupancy · NPS · GOP proxy |
| `metric_view_hotel_kpis` | UC Metric View | YAML semantic layer over Gold |
| `[dev] Brickstar Hotel KPIs` | Lakeview dashboard | Brickstar-styled · cosmos/turquoise/sage/amber roles |
| Genie Space `Brickstar Hotel Performance` | Post-deploy | NL → SQL on Gold + Metric View · Brickstar code-skill in the description |

Detailed design: **[`docs/Architecture.md`](docs/Architecture.md)**

## Prerequisites

### System (must be present on a fresh desktop)
| Tool | Min version | Notes |
|---|---|---|
| `bash`, `curl`, `git` | any | Standard on macOS / most Linux distros |
| `python3` | 3.10+ | System Python is fine |

`setup.sh` installs the rest automatically:

| Tool | Min version | Auto-install via |
|---|---|---|
| [`uv`](https://github.com/astral-sh/uv) | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Databricks CLI | 0.281 | `brew install databricks/tap/databricks` (macOS) · `setup-cli/install.sh` (Linux) |
| `databricks-sdk` | 0.50+ | `uv pip install -r requirements.txt` |

### Workspace (must be configured by an admin)
- Premium tier (UC + Metric Views + Genie)
- A UC catalog the user can write into (default `classic_stable_89j9qf`)
- Permission to create SQL warehouses (the bundle provisions a Serverless PRO Medium warehouse on first deploy)
- A CLI profile pointing at the workspace (default `brickstar`)

### Create the `brickstar` CLI profile (one-time)

The setup script will tell you if the profile is missing — here's how to create
it once:

1. Find your workspace URL. From the Databricks workspace UI, copy the address
   bar root (everything before `/?o=…`). It looks like:

   ```
   https://adb-1234567890123456.7.azuredatabricks.net          # Azure
   https://dbc-abc12345-def6.cloud.databricks.com              # AWS
   https://1234567890123456.7.gcp.databricks.com               # GCP
   ```

2. Run the OAuth login (browser opens, you accept; **no token to copy**):

   ```bash
   databricks auth login \
     --host https://<your-workspace-url> \
     --profile brickstar
   ```

3. Verify:

   ```bash
   databricks current-user me --profile brickstar
   ```

The profile is stored in `~/.databrickscfg` and is picked up automatically by
every command in this project (the bundle's `databricks.yml` has
`workspace.profile: brickstar` hard-wired for the `dev` target).

For Personal Access Token (PAT) auth instead of OAuth, use
`databricks configure --token --profile brickstar` and paste the token when
prompted.

## Reproduce from scratch

```bash
git clone <your-repo-url>
cd brickstar_validation

# 0 · One-shot bootstrap (installs uv + Databricks CLI if missing, creates
#     .venv with uv, installs deps, verifies profile/catalog/warehouse)
./setup.sh

# Activate the venv (setup.sh prints this hint at the end)
source .venv/bin/activate

# 1 · Validate the bundle (parses YAML, resolves variables/refs, no API call)
databricks bundle validate -t dev

# 2 · Deploy schema + volume + pipeline + job + dashboard
databricks bundle deploy -t dev --auto-approve

# 3 · Run the orchestrator job · ~16 min on first run
#     seed (~100s) → fan-out (~12 min) → SDP (~3 min) → metric view (~30s)
databricks bundle run brickstar_validation_pipeline -t dev

# 4 · (Optional) Mirror the Brickstar style skill to /Workspace/Shared/
#     so Genie and future dashboards can reference it
./src/scripts/upload_brickstar_skill.sh brickstar

# 5 · Create the Genie Space (auto-detects the dev-mode prefixed schema)
python src/scripts/create_genie_space.py
```

`setup.sh` accepts overrides via env vars or flags:

```bash
PROFILE=my-profile CATALOG=my_catalog WAREHOUSE_ID=abc ./setup.sh
./setup.sh --profile my-profile --skip-auth     # CI / pre-configured envs
```

Expected output of step 6:

```
Catalog: classic_stable_89j9qf
Schema:  dev_<username>_brickstar_validation
Warehouse: 5fa9027b3b6ad408
Title:   Brickstar Hotel Performance

✓ Genie Space created: 01f...
  Open: https://<workspace-host>/genie/rooms/01f...
```

## Verify

Once step 4 succeeds, smoke-test the Metric View (resolve the warehouse id
created by the bundle first):

```bash
WAREHOUSE_ID=$(databricks warehouses list --profile brickstar --output json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print([w['id'] for w in d if 'Brickstar Serverless Warehouse' in w.get('name','')][0])")

databricks api post /api/2.0/sql/statements --profile brickstar --json "$(cat <<JSON
{
  "warehouse_id": "$WAREHOUSE_ID",
  "statement": "SELECT MEASURE(total_revenue_eur) AS revenue, MEASURE(portfolio_occupancy_pct) AS occupancy_pct, MEASURE(portfolio_revpar_eur) AS revpar FROM classic_stable_89j9qf.dev_<username>_brickstar_validation.metric_view_hotel_kpis",
  "wait_timeout": "30s"
}
JSON
)"
```

Open the dashboard and the Genie Space:

```bash
WORKSPACE_HOST=https://adb-7405604561430667.7.azuredatabricks.net   # update for your workspace

DASHBOARD_ID=$(databricks lakeview list --profile brickstar --output json \
  | python3 -c "import json,sys; data=json.load(sys.stdin); items=data if isinstance(data,list) else data.get('dashboards',[]); print([d['dashboard_id'] for d in items if 'Brickstar' in d.get('display_name','')][0])")
open "$WORKSPACE_HOST/dashboardsv3/$DASHBOARD_ID/published"

SPACE_ID=$(databricks genie list-spaces --profile brickstar --output json \
  | python3 -c "import json,sys; data=json.load(sys.stdin); spaces=data.get('spaces',data) if isinstance(data,dict) else data; print([s.get('space_id', s.get('id')) for s in spaces if 'Brickstar Hotel Performance' in s.get('title','')][0])")
open "$WORKSPACE_HOST/genie/rooms/$SPACE_ID"
```

## Cleanup

```bash
# Tear down everything (schema, volume, pipeline, job, dashboard, Bronze/Silver/Gold)
databricks bundle destroy -t dev --auto-approve

# Genie Space lives outside the bundle — delete separately
databricks genie trash-space <SPACE_ID> --profile brickstar
```

## Configuration

All knobs live in `databricks.yml` variables:

| Variable | Default | Description |
|---|---|---|
| `catalog` | `classic_stable_89j9qf` | UC catalog (must exist) |
| `schema` | `brickstar_validation` | UC schema (created by bundle; **dev-mode prefixes to `dev_<user>_brickstar_validation`**) |
| `volume_name` | `raw_http` | UC volume for RAW JSON |
| `num_hotels` | `30` | Hotels to simulate |
| `history_days` | `60` | Days of history to seed |

The SQL warehouse used by the dashboard and Genie Space is provisioned by
`resources/warehouse.yml` (Serverless PRO Medium · Photon · auto-stop 10 min)
— no configuration needed.

Override per-deploy:

```bash
databricks bundle deploy -t dev --var "catalog=my_catalog" --var "num_hotels=10"
```

## Project structure

```
brickstar_validation/
├── databricks.yml                       # Bundle config + variables + target
├── README.md                            # This file
├── setup.sh                             # One-shot bootstrap (uv + CLI + venv + auth checks)
├── requirements.txt                     # databricks-sdk for post-deploy scripts
├── .gitignore                           # Ignores .databricks/, .venv/, __pycache__/, .DS_Store
├── docs/
│   └── Architecture.md                  # Full design doc · governance · migration to prod
├── resources/                           # DAB resource definitions
│   ├── schemas.yml                      # UC schema
│   ├── volume.yml                       # UC volume
│   ├── warehouse.yml                    # Serverless PRO Medium warehouse
│   ├── pipeline.yml                     # Lakeflow SDP serverless
│   ├── job.yml                          # 4-task orchestrator
│   └── dashboard.yml                    # Lakeview dashboard
└── src/
    ├── notebooks/
    │   ├── 00_seed_mock_sources.py      # Synthetic hotels + reservations + control_endpoints
    │   ├── 01_mock_http_fanout.py       # ~50 endpoint payloads / hotel / day → UC Volume
    │   └── 02_create_metric_view.py     # CREATE VIEW WITH METRICS LANGUAGE YAML
    ├── pipelines/
    │   └── transformations/             # Discovered by SDP via libraries.glob
    │       ├── bronze.py                # Auto Loader · cloudFiles · streaming
    │       ├── silver.py                # Expectations · dedup · status normalisation
    │       └── gold.py                  # Daily KPIs · RevPAR/ADR/occupancy/NPS/GOP
    ├── metric_views/
    │   └── hotel_kpis.yml               # Reference YAML (mirror of embedded)
    ├── dashboards/
    │   └── brickstar_kpis.lvdash.json   # Brickstar-styled Lakeview
    └── scripts/
        ├── create_genie_space.py        # Post-deploy · creates Genie Space
        └── upload_brickstar_skill.sh    # Mirror Brickstar style to /Workspace/Shared/
```

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `[SCHEMA_NOT_FOUND] ...brickstar_validation` during job run | Notebook received unprefixed `${var.schema}` while DAB dev-mode created `dev_<user>_brickstar_validation` | Already fixed — every `base_parameters.schema*` in `resources/job.yml`, `resources/pipeline.yml`, `resources/dashboard.yml` uses `${resources.schemas.brickstar.name}` |
| `PATH_NOT_FOUND .../raw_http/**/metrics.json` in fan-out task | Spark glob `**` does not expand to multiple directory levels | Already fixed — sanity read uses `.option("recursiveFileLookup","true")` |
| `Invalid serialized_space: Unknown field 'general_instructions'` when creating the Genie Space | Genie API accepts a minimal config schema only | Already fixed — script puts the Brickstar instructions in the `description` kwarg, not in `serialized_space` |
| Dashboard widgets show "table not found" | Dashboard queries had a hardcoded schema prefix | Already fixed — queries use unqualified names + `dataset_schema: ${resources.schemas.brickstar.name}` |
| Job task fails with `bundle is not defined` | A `$` in code/YAML inside a JS template-literal (e.g. dashboard JSON) was eaten as an interpolation | Escape with `\$` in any literal `${...}` you want preserved |

## License

Internal Databricks Field Engineering sample. Not for external distribution.
