# Architecture · Brickstar Hotel Daily Performance

End-to-end design of the reference validation slice: **two source patterns**
(managed CDC + custom HTTP fan-out), **Lakebase-style control table**, **UC
Volume RAW landing**, **Lakeflow Spark Declarative Pipeline (SDP) with Auto
Loader**, **UC Metric View**, **AI/BI Lakeview dashboard** styled with
Brickstar, and a **Genie Space** as the NL → SQL interface.

---

## 1 · Goal

Validate, on real Databricks infrastructure, the **hybrid ingest pattern +
Medallion governance + Metric View + AI/BI + Genie** stack — using **mocked**
versions of the SQL Server CDC source and the HTTP web service so the slice
runs without any external dependency.

This is the smallest deployable unit that exercises every Databricks feature
in the target architecture:

| Pattern | Implemented by |
|---|---|
| Lakeflow Connect for SQL Server (CDC) | `00_seed_mock_sources.py` (synthetic data → Delta tables that simulate the CDC output) |
| Lakebase Postgres control table | `control_endpoints` Delta table (Lakebase-equivalent semantics) |
| Lakeflow Job + Notebook fan-out (~50 endpoints / hotel) | `01_mock_http_fanout.py` (writes JSON payloads to a UC Volume in the same partition shape as the real fan-out) |
| UC Volume RAW landing | `raw_http` MANAGED volume |
| Auto Loader (event-driven) | SDP table `bronze_web_metrics` using `cloudFiles` |
| Lakeflow SDP (Bronze/Silver/Gold) | `brickstar_medallion` serverless SDP pipeline |
| Liquid Clustering | `cluster_by=...` on every SDP table |
| Expectations DQ | `@dlt.expect_or_drop` / `@dlt.expect_or_fail` in Silver |
| UC Metric View (1 KPI · 1 definition) | `metric_view_hotel_kpis` (YAML inside `CREATE VIEW WITH METRICS`) |
| AI/BI Lakeview dashboard | `brickstar_kpis.lvdash.json` styled with Brickstar tokens |
| Genie Space (NL → SQL) | Post-deploy `create_genie_space.py` + Brickstar code-skill in the description |

---

## 2 · Logical layers

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SOURCES (mocked)                                                             │
│  ┌──────────────────────────┐    ┌────────────────────────────────────────┐  │
│  │ SQL Server mock           │    │ HTTP web service mock                  │  │
│  │ • hotels                  │    │ • simulates ~50 endpoints / hotel     │  │
│  │ • reservations            │    │ • returns daily JSON per hotel        │  │
│  └─────────────┬─────────────┘    └──────────────────────┬──────────────────┘ │
│                │ (Delta tables)                            │ (Notebook writes  │
│                │                                           │  JSON to Volume)  │
└────────────────┼───────────────────────────────────────────┼──────────────────┘
                 │                                           │
                 │       ┌───────────────────────────────────┘
                 │       │
┌────────────────┼───────┼──────────────────────────────────────────────────────┐
│  INGEST                                                                         │
│  ┌─────────────▼───┐   ┌──────▼────────────────────────────────────────────┐  │
│  │ direct Delta    │   │ UC Volume raw_http                                 │  │
│  │ (CDC output     │   │ /Volumes/<cat>/<schema>/raw_http/                  │  │
│  │  proxy)         │   │   hotel_id=X/year=Y/month=M/day=D/metrics.json     │  │
│  └─────────────┬───┘   └──────┬────────────────────────────────────────────┘  │
│                │              │                                                │
│        ┌───────▼──────────────▼─────────────────────────────────────────────┐ │
│        │ Lakeflow Job · `brickstar_validation_pipeline`                     │ │
│        │   seed → fan-out → SDP pipeline → metric view                       │ │
│        └────────────────────────┬────────────────────────────────────────────┘ │
└─────────────────────────────────┼──────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────┼──────────────────────────────────────────────┐
│  MEDALLION (SDP serverless · Photon)                                            │
│                                  │                                              │
│   Bronze ─────────────────────────────────────────────────────────────────►    │
│     • bronze_web_metrics (Auto Loader, cloudFiles · streaming)                  │
│                                                                                 │
│   Silver                                                                        │
│     • silver_hotels  (clean + expectations)                                     │
│     • silver_reservations  (clean + expectations + cancellations filtered)      │
│     • silver_daily_web_metrics  (deduped, watermarked)                          │
│                                                                                 │
│   Gold                                                                          │
│     • gold_hotel_daily_kpis  (RevPAR, ADR, occupancy, NPS, GOP proxy)           │
│                                                                                 │
└─────────────────────────────────┬──────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼──────────────────────────────────────────────┐
│  GOVERNED METRIC LAYER (UC Metric View)                                         │
│  metric_view_hotel_kpis  · dimensions (date, hotel, country, market_segment)    │
│                          · measures (revenue, RevPAR, occupancy, ADR, NPS,…)    │
└─────────────────────────────────┬──────────────────────────────────────────────┘
                                  │
       ┌──────────────────────────┴──────────────────────────────┐
       │                                                          │
┌──────▼────────────────────────┐               ┌─────────────────▼──────────────┐
│  AI/BI Lakeview dashboard      │               │  Genie Space (NL → SQL)        │
│  brickstar_kpis (Brickstar)    │               │  "Brickstar Hotel Performance" │
│  KPIs + trends + top-N + table │               │  description = Brickstar skill │
└────────────────────────────────┘               └────────────────────────────────┘
```

All Bronze/Silver/Gold + Volume + Metric View live inside a **single UC
schema** (`brickstar_validation`). Under DAB `mode: development` the deployed
schema name is automatically prefixed to `dev_<username>_brickstar_validation`
— every resource reference in the bundle uses
`${resources.schemas.brickstar.name}` to resolve to the actual prefixed name.
See §5.

---

## 3 · Use case · *Brickstar Hotel Daily Performance*

30 simulated hotels × 60 days of history × ~50 endpoint payloads per
hotel-day = roughly 1,800 raw JSON files in the Volume plus ~1,800
reservation-aggregated Gold rows.

Hotels are seeded across 15 destinations (Mallorca, Ibiza, Cancún, Punta
Cana, …) and 5 market segments (Beachfront Luxury, All-Inclusive, Family
Resort, Urban, Wellness). Occupancy patterns include weekend boost,
segment-specific base rates, and reasonable random noise.

The Gold fact `gold_hotel_daily_kpis` carries:

| Column | Meaning |
|---|---|
| `metric_date`, `hotel_id` | Grain |
| `hotel_name`, `country`, `destination`, `market_segment`, `stars` | Dimensions |
| `rooms_total`, `rooms_occupied`, `occupancy_pct` | Capacity |
| `adr_eur`, `daily_revenue_eur`, `revpar_eur` | Revenue KPIs |
| `walkins`, `cancellations` | Operational |
| `nps_responses`, `nps_score` | Guest sentiment |
| `booked_revenue_eur`, `reservations_count`, `avg_booking_adr_eur` | Reservations cross-check |
| `gop_proxy_eur` | Margin proxy (40-65% depending on segment) |

The Metric View exposes a curated set of measures so PBI, AI/BI, and Genie
all consume one definition:

| Measure | Expression |
|---|---|
| `total_revenue_eur` | `SUM(daily_revenue_eur)` |
| `avg_revpar_eur` | `AVG(revpar_eur)` |
| `avg_adr_eur` | `AVG(adr_eur)` |
| `avg_occupancy_pct` | `AVG(occupancy_pct)` |
| `total_rooms_occupied` / `total_rooms_available` | SUMs |
| `total_reservations`, `total_cancellations` | SUMs |
| `avg_nps_score` | `AVG(nps_score)` |
| `total_gop_proxy_eur` | `SUM(gop_proxy_eur)` |
| `portfolio_occupancy_pct` | `SUM(rooms_occupied) * 100.0 / NULLIF(SUM(rooms_total), 0)` |
| `portfolio_revpar_eur` | `SUM(daily_revenue_eur) / NULLIF(SUM(rooms_total), 0)` |

---

## 4 · Resources (provisioned by DAB)

```
classic_stable_89j9qf                                    [pre-existing, NOT created]
  └── dev_<user>_brickstar_validation                    SCHEMA   ← created
      ├── raw_http                                       VOLUME   ← created
      │
      ├── hotels                                         TABLE      ← seed notebook
      ├── reservations                                   TABLE      ← seed notebook
      ├── control_endpoints                              TABLE      ← seed notebook
      │
      ├── bronze_web_metrics                             STREAMING_TABLE ← SDP
      ├── silver_hotels                                  MAT_VIEW         ← SDP
      ├── silver_reservations                            MAT_VIEW         ← SDP
      ├── silver_daily_web_metrics                       STREAMING_TABLE  ← SDP
      ├── gold_hotel_daily_kpis                          MAT_VIEW         ← SDP
      │
      └── metric_view_hotel_kpis                         METRIC_VIEW ← create_metric_view notebook

PIPELINES
  └── [dev <user>] brickstar_medallion                   serverless SDP, Photon

JOBS
  └── [dev <user>] brickstar_validation_pipeline         4 tasks (seed → fan-out → SDP → metric-view)

SQL WAREHOUSES
  └── [dev <user>] Brickstar Serverless Warehouse        Serverless PRO Medium · Photon · auto-stop 10 min

DASHBOARDS
  └── [dev <user>] [dev] Brickstar Hotel KPIs            Lakeview · Brickstar styled · uses the warehouse above
```

The Genie Space (`Brickstar Hotel Performance`) is created **post-deploy** by
`src/scripts/create_genie_space.py` — DAB does not yet expose a `genie_spaces`
resource type.

---

## 5 · DAB `mode: development` and schema prefixing

In `databricks.yml` the dev target sets `mode: development`. Databricks Asset
Bundles automatically prepends `dev_<username>_` to user-deployable resources
so multiple developers can share one catalog without collisions:

| Resource | Variable value | Deployed name (example) |
|---|---|---|
| Schema | `brickstar_validation` | `dev_alice_doe_brickstar_validation` |
| Volume | `raw_http` | `raw_http` (NOT prefixed) |
| Pipeline | `brickstar_medallion` | `[dev alice_doe] brickstar_medallion` |
| Job | `brickstar_validation_pipeline` | `[dev alice_doe] [dev] brickstar_validation_pipeline` |
| Dashboard | `Brickstar Hotel KPIs` | `[dev alice_doe] [dev] Brickstar Hotel KPIs` |

**Implication:** every notebook task that needs the schema name receives it
via DAB resource reference, not the variable. The bundle uses
`${resources.schemas.brickstar.name}` everywhere a notebook / pipeline /
dashboard needs the actual deployed schema:

```yaml
# resources/job.yml — correct
base_parameters:
  catalog: ${var.catalog}
  schema_bronze: ${resources.schemas.brickstar.name}   # resolves at deploy
```

Using `${var.schema}` (the original-form variable) would cause
`SCHEMA_NOT_FOUND` at runtime — this is the most common gotcha when porting a
non-dev-mode bundle to dev-mode.

The post-deploy `create_genie_space.py` script auto-detects the prefixed
schema by listing UC schemas under the catalog and matching the suffix.

---

## 6 · Why each Databricks feature is in the stack

### 6.1 · Mocked SQL Server `hotels` + `reservations` (instead of Lakeflow Connect)

In production, **Lakeflow Connect SQL Server CDC** writes Bronze tables
directly to UC. Here we substitute with a Spark notebook that generates
synthetic data and `saveAsTable` into the same schema — the rest of the
pipeline doesn't know it's mocked. Switching to the real connector means
**removing the `seed_mock_sources` task** and adding a Lakeflow Connect
ingestion pipeline targeting the same table names.

### 6.2 · `control_endpoints` Delta table (instead of Lakebase Postgres)

Carries the *which endpoints to call per active hotel* mapping. In production
this lives in Lakebase Postgres for low-latency lookups and transactional
updates. The Delta version preserves identical semantics for the fan-out
notebook: `SELECT endpoint, params WHERE active = true`. Migration to
Lakebase means swapping the table connection string and adding a synced-table
mirror for Spark-side joins.

### 6.3 · `01_mock_http_fanout.py` notebook

Demonstrates the **fan-out pattern**: read the control table → loop hotels →
loop days → simulate ~50 invocations (in production: `asyncio.gather` of 50
real HTTPS calls with retries) → write payload JSON to a UC Volume. Replacing
the simulation with real HTTP keeps the same partition shape
`hotel_id=X/year=Y/month=M/day=D/metrics.json` so Auto Loader downstream is
unaffected.

### 6.4 · UC Volume `raw_http`

Acts as the **landing zone** between fan-out and SDP. Benefits over
Bronze-direct write:
- Original JSON payload archived → debug and reprocess without re-calling the source.
- GDPR retention configurable per-volume.
- Auto Loader's incremental processing decouples ingest cadence from transformation cadence.
- The Volume name does **not** get a dev prefix (UC Volume names live under the prefixed schema, not in their own namespace).

### 6.5 · Lakeflow SDP (`brickstar_medallion`)

**Serverless, Photon-enabled, declarative**. Each table is a `@dlt.table`
Python function:
- **Bronze** (`bronze.py`) — Auto Loader stream on `cloudFiles` from the Volume.
- **Silver** (`silver.py`) — `@dlt.expect_or_drop` for data quality (valid dates, positive revenue, known channel) + dedup + status normalisation. Reads the seed-managed `hotels`/`reservations` tables directly via `spark.read.table()` (these are not DLT-managed).
- **Gold** (`gold.py`) — joins Silver with metrics, computes RevPAR (`revenue / rooms`), enforces ranges, derives GOP proxy by segment.

Every SDP table declares `cluster_by=["hotel_id", "metric_date"]` (or
analog) → Liquid Clustering takes over; no manual `OPTIMIZE` needed thanks to
Predictive Optimization.

### 6.6 · Metric View `metric_view_hotel_kpis`

UC-native: queryable from any engine (Spark, DBSQL, PBI, Genie) with
`MEASURE(...)` syntax. Single source of truth for portfolio-level KPIs:

```sql
SELECT MEASURE(total_revenue_eur), MEASURE(portfolio_occupancy_pct)
FROM classic_stable_89j9qf.dev_<user>_brickstar_validation.metric_view_hotel_kpis
WHERE metric_date >= date_sub(current_date, 30)
```

Created via `CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML AS $$...$$;`
in `02_create_metric_view.py` — the same YAML is mirrored at
`src/metric_views/hotel_kpis.yml` for human inspection.

### 6.7 · AI/BI Lakeview dashboard

JSON-defined Lakeview dashboard (`brickstar_kpis.lvdash.json`). All datasets
query the Metric View with `MEASURE(...)` aggregations — never raw Gold — so
business logic stays governed.

The dashboard is styled per the **Brickstar dashboard style** (see
`~/.claude/skills/brickstar-dashboard-style/` or
`/Workspace/Shared/skills/brickstar-dashboard-style/`). Role → color mapping:

| Role | Color | Where |
|---|---|---|
| Primary monetary | `#002855` cosmos navy | Revenue total counter, top-hotels bar |
| Core operational KPI | `#3AA597` turquoise | Occupancy, RevPAR counters, revenue trend line |
| Heritage / sustainable | `#A6C26E` sage | ADR counter, occupancy area trend |
| Neutral attention | `#FFD54F` amber | NPS counter |
| Warm activity | `#EE8E00` carrot | Occupancy by destination bar |

`dataset_schema` in `resources/dashboard.yml` is bound to
`${resources.schemas.brickstar.name}` so dashboard SQL refers to unqualified
table/view names — and the dev-mode prefix is resolved at deploy time.

### 6.8 · Genie Space

Created post-deploy by `create_genie_space.py`. Datasets are the Gold fact
and the Metric View. The space description embeds the Brickstar style guide
so future dashboard / chart suggestions from Genie inherit the same visual
language. Sample questions cover RevPAR analysis, segment comparisons, NPS
alerts, and explicit Brickstar dashboard-generation prompts.

---

## 7 · Job orchestration

`brickstar_validation_pipeline` (defined in `resources/job.yml`) is a 4-task
DAG:

```
seed_mock_sources                       (~100s · notebook · synthetic data)
   ↓
mock_http_fanout                        (~10-12 min · notebook · ~1.8K JSON files)
   ↓
run_medallion_pipeline                  (~2-3 min · SDP serverless run, full_refresh: true)
   ↓
create_metric_view                      (~30-40s · notebook · CREATE VIEW)
```

Total wall-clock: ~16 minutes on first run. Subsequent runs (no full_refresh,
Auto Loader incremental) drop to ~3-5 min.

For production cadence (e.g. two runs per day) replace the job's manual
trigger with:

```yaml
schedule:
  quartz_cron_expression: "0 0 6,18 * * ?"
  timezone_id: "Europe/Madrid"
```

---

## 8 · Brickstar dashboard style as a Genie code skill

The Brickstar style skill is dual-located:

| Location | Purpose |
|---|---|
| `~/.claude/skills/brickstar-dashboard-style/` (local) | Used by the Claude Code agent when authoring dashboards |
| `/Workspace/Shared/skills/brickstar-dashboard-style/` (workspace) | Referenced by Genie Space description so Genie applies the style when generating visualisations |

Upload via:

```bash
./src/scripts/upload_brickstar_skill.sh brickstar
```

The Genie Space `description` field includes the absolute workspace paths
plus an inlined cheat sheet of color tokens, font roles, and anti-patterns.
This is the closest analogue available today to a Databricks *Genie code
skill* (no first-class skill resource is exposed via the public API as of CLI
0.281).

---

## 9 · Governance model

| Concern | Implementation |
|---|---|
| Catalog isolation | All resources under one UC catalog (`classic_stable_89j9qf`) · pre-existing |
| Per-user dev isolation | DAB `mode: development` → schema name prefixed with `dev_<username>_` |
| Lineage | Auto column-level lineage via UC for all SDP tables and the Metric View |
| Audit | `system.access.audit` captures every query against Bronze/Silver/Gold/Metric View |
| Permissions | Bundle grants `users` group `CAN_VIEW` on pipeline, `CAN_RUN` on dashboard, `CAN_MANAGE_RUN` on job |
| PII | None in the synthetic data; production replacement should add UC `column_mask` + `row_filter` on `silver_reservations.guests` if guest PII enters |

---

## 10 · From sample to production · migration checklist

1. **Source replacement**
   - Remove `seed_mock_sources` task.
   - Provision Lakeflow Connect for SQL Server (CDC log-based) → writes directly to `bronze_hotels`, `bronze_reservations`.
   - Adjust Silver to read from these Bronze tables (rename `hotels` → `bronze_hotels`, etc.).
2. **HTTP fan-out**
   - Replace `01_mock_http_fanout.py` simulation with real `httpx.AsyncClient` fan-out against the actual web service.
   - Move `control_endpoints` from Delta to **Lakebase Postgres** (Lakebase managed Postgres provisioned + synced-table mirror to UC for Spark-side joins).
3. **Schedule**
   - Add `schedule.quartz_cron_expression: "0 0 6,18 * * ?"` to the Job (two runs/day).
4. **Bundle target**
   - Switch from `dev` to `prod` (drop `mode: development`, set explicit catalog/schema names without prefix).
5. **Premium tier**
   - Ensure the workspace has Premium tier (required for UC, Metric Views, and Genie).
6. **Monitoring**
   - Add DBSQL alerts on `metric_view_hotel_kpis` (RevPAR drop > 15% week-over-week, NPS < 50, daily cancellations > threshold).
7. **Cost**
   - Tag the job, pipeline, and warehouse with `cost_center=brickstar_prod` for `system.billing.usage` granularity.

---

## 11 · References

- [`../README.md`](../README.md) — quick-start, deploy, verify, cleanup
- [`../src/scripts/create_genie_space.py`](../src/scripts/create_genie_space.py) — Genie Space creator (auto-detects prefixed schema)
- [`../src/scripts/upload_brickstar_skill.sh`](../src/scripts/upload_brickstar_skill.sh) — mirror Brickstar style skill to the workspace
- [Brickstar dashboard style skill](../../../../../../../.claude/skills/brickstar-dashboard-style/SKILL.md) — token reference + role-color mapping
- [Databricks Asset Bundles docs](https://docs.databricks.com/dev-tools/bundles/)
- [Lakeflow Spark Declarative Pipelines docs](https://docs.databricks.com/en/delta-live-tables/index.html)
- [UC Metric Views docs](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-metric-view.html)
- [Genie Spaces docs](https://docs.databricks.com/en/genie/index.html)
