"""Create the Brickstar Genie Space programmatically (post-deploy step).

DAB does not yet have a first-class `genie_spaces` resource (CLI 0.281+), so
this script is invoked manually after `databricks bundle deploy`.

It auto-detects the DAB development-mode schema prefix (e.g. the actual schema
name is `dev_<username>_brickstar_validation` not `brickstar_validation`) by
querying Unity Catalog and matching the suffix.

Usage:
    python3 src/scripts/create_genie_space.py
    python3 src/scripts/create_genie_space.py --profile brickstar
    python3 src/scripts/create_genie_space.py --catalog X --schema Y    # override auto-detect

Requirements:
    pip install databricks-sdk
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Optional

from databricks.sdk import WorkspaceClient


SAMPLE_QUESTIONS = [
    "¿Cuál fue el RevPAR medio del portfolio el mes pasado?",
    "Top 5 hoteles por revenue total esta semana",
    "Compara la ocupación entre All-Inclusive y Family Resort en el último trimestre",
    "¿Qué destinos tuvieron NPS por debajo de 60 en mayo?",
    "Tendencia de ADR diario para Brickstar Selection Mallorca",
    "¿Cuál es el GOP proxy total por país?",
    "Hoteles con caída de ocupación >10% mes contra mes",
    "Genera un dashboard de portfolio aplicando el estilo Brickstar (turquoise + cosmos)",
]

DESCRIPTION = """Q&A sobre KPIs diarios de hoteles Brickstar — RevPAR, ADR, ocupación, NPS, GOP proxy. Consume la Metric View gobernada en UC.

## Code skill: Brickstar dashboard style (attached)
Cuando generes dashboards, gráficos o KPIs desde este space, sigue la guía Brickstar en:
- /Workspace/Shared/skills/brickstar-dashboard-style/SKILL.md
- /Workspace/Shared/skills/brickstar-dashboard-style/style-guide.md
- /Workspace/Shared/skills/brickstar-dashboard-style/dashboard-template.lvdash.json

Reglas clave:
* Primary KPI / brand color: #3AA597 (turquoise). Counter value color: #002855 (cosmos).
* Headlines: serif (Noe Display, fallback Georgia) · weight 500.
* Body: Inter / system-ui · weight 400.
* Page background: #F6F8FC (nunca blanco puro).
* Series palette (ordenada): #3AA597 → #002855 → #A6C26E → #EE8E00 → #524BB9 → #31C1E7.
* Alert (cancellations / NPS<50): #EF5350 sólo si es realmente "alert".
* NO uses Databricks orange (#FF3621) en este space."""


def detect_schema(w: WorkspaceClient, catalog: str, suffix: str) -> Optional[str]:
    """Find the deployed schema by suffix (handles DAB dev-mode prefix `dev_<user>_`)."""
    candidates = []
    for s in w.schemas.list(catalog_name=catalog):
        if s.name == suffix or s.name.endswith("_" + suffix):
            candidates.append(s.name)
    if not candidates:
        return None
    # exact match wins over prefix match
    candidates.sort(key=lambda n: (n != suffix, len(n)))
    return candidates[0]


def detect_warehouse(w: WorkspaceClient, name_suffix: str) -> Optional[str]:
    """Find the bundle-deployed warehouse by name suffix.

    DAB dev-mode renames warehouses with a `[dev <user>]` prefix, so we match
    on suffix instead of exact name.
    """
    candidates = []
    for wh in w.warehouses.list():
        if wh.name and wh.name.endswith(name_suffix):
            candidates.append((wh.name, wh.id))
    if not candidates:
        return None
    # Shortest name wins (closest to exact match)
    candidates.sort(key=lambda t: len(t[0]))
    return candidates[0][1]


def build_serialized_space(catalog: str, schema: str) -> str:
    """Build the minimal valid serialized_space payload.

    Verified against /api/2.0/genie/spaces. Only `version`, `config.sample_questions`
    and `data_sources.tables` are accepted at the top level — instruction text goes
    in the `description` kwarg of create_space().
    """
    payload = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS
            ],
        },
        "data_sources": {
            "tables": [
                {"identifier": f"{catalog}.{schema}.gold_hotel_daily_kpis"},
                {"identifier": f"{catalog}.{schema}.metric_view_hotel_kpis"},
            ],
        },
    }
    return json.dumps(payload)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="brickstar", help="~/.databrickscfg profile")
    parser.add_argument("--warehouse-id", default=None,
                        help="Override warehouse auto-detection · pass an explicit id")
    parser.add_argument("--warehouse-name-suffix", default="Brickstar Serverless Warehouse",
                        help="Substring matched against the bundle-deployed warehouse name")
    parser.add_argument("--catalog", default="classic_stable_89j9qf")
    parser.add_argument("--schema-suffix", default="brickstar_validation",
                        help="Schema name suffix to look for (handles DAB dev-mode prefix)")
    parser.add_argument("--schema", default=None,
                        help="Override auto-detection · use exact schema name")
    parser.add_argument("--title", default="Brickstar Hotel Performance")
    parser.add_argument("--parent-path", default="/Workspace/Shared",
                        help="Folder for the space (must already exist)")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)

    schema = args.schema or detect_schema(w, args.catalog, args.schema_suffix)
    if not schema:
        print(f"❌ No schema matching suffix '{args.schema_suffix}' under {args.catalog}",
              file=sys.stderr)
        print(f"   Deploy the bundle first: `databricks bundle deploy -t dev --profile {args.profile}`",
              file=sys.stderr)
        return 1

    warehouse_id = args.warehouse_id or detect_warehouse(w, args.warehouse_name_suffix)
    if not warehouse_id:
        print(f"❌ No warehouse matching suffix '{args.warehouse_name_suffix}'", file=sys.stderr)
        print(f"   Deploy the bundle first: `databricks bundle deploy -t dev --profile {args.profile}`",
              file=sys.stderr)
        return 1

    print(f"Catalog: {args.catalog}")
    print(f"Schema:  {schema}")
    print(f"Warehouse: {warehouse_id}")
    print(f"Title:   {args.title}")
    print()

    serialized = build_serialized_space(args.catalog, schema)

    try:
        space = w.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized,
            title=args.title,
            description=DESCRIPTION,
            parent_path=args.parent_path,
        )
        host = w.config.host.rstrip("/")
        print(f"✓ Genie Space created: {space.space_id}")
        print(f"  Open: {host}/genie/rooms/{space.space_id}")
        return 0
    except Exception as e:
        print(f"\n❌ create_space failed: {e}", file=sys.stderr)
        print(
            "\nFallback (manual): open the workspace UI → SQL → Genie → New Space.\n"
            f"  Datasets:\n"
            f"    {args.catalog}.{schema}.gold_hotel_daily_kpis\n"
            f"    {args.catalog}.{schema}.metric_view_hotel_kpis\n"
            f"  Warehouse: {warehouse_id}\n"
            f"  Sample questions / description: copy from this script.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
