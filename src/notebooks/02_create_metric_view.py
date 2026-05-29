# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Create / replace the Metric View on Gold

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_89j9qf")
dbutils.widgets.text("schema", "brickstar_validation")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

view_fqn = f"{catalog}.{schema}.metric_view_hotel_kpis"
source_fqn = f"{catalog}.{schema}.gold_hotel_daily_kpis"

# COMMAND ----------

# Embedded YAML keeps the resource self-contained at deploy time. The same YAML
# also lives at ../metric_views/hotel_kpis.yml for human review.
yaml_body = f"""version: 0.1
source: {source_fqn}

dimensions:
  - name: metric_date
    expr: metric_date
  - name: hotel_id
    expr: hotel_id
  - name: hotel_name
    expr: hotel_name
  - name: country
    expr: country
  - name: destination
    expr: destination
  - name: market_segment
    expr: market_segment
  - name: stars
    expr: stars

measures:
  - name: total_revenue_eur
    expr: SUM(daily_revenue_eur)
  - name: avg_revpar_eur
    expr: AVG(revpar_eur)
  - name: avg_adr_eur
    expr: AVG(adr_eur)
  - name: avg_occupancy_pct
    expr: AVG(occupancy_pct)
  - name: total_rooms_occupied
    expr: SUM(rooms_occupied)
  - name: total_rooms_available
    expr: SUM(rooms_total)
  - name: total_reservations
    expr: SUM(reservations_count)
  - name: total_cancellations
    expr: SUM(cancellations)
  - name: avg_nps_score
    expr: AVG(nps_score)
  - name: total_gop_proxy_eur
    expr: SUM(gop_proxy_eur)
  - name: portfolio_occupancy_pct
    expr: SUM(rooms_occupied) * 100.0 / NULLIF(SUM(rooms_total), 0)
  - name: portfolio_revpar_eur
    expr: SUM(daily_revenue_eur) / NULLIF(SUM(rooms_total), 0)
"""

ddl = f"""CREATE OR REPLACE VIEW {view_fqn}
WITH METRICS
LANGUAGE YAML
COMMENT 'Brickstar daily hotel KPIs · governed metric layer for PBI / AI-BI / Genie'
AS $${yaml_body}$$
"""

print(f"Creating Metric View {view_fqn} on top of {source_fqn}")
spark.sql(ddl)

# COMMAND ----------

display(spark.sql(f"""
  SELECT
    MEASURE(total_revenue_eur)      AS total_revenue,
    MEASURE(portfolio_occupancy_pct) AS occupancy_pct,
    MEASURE(portfolio_revpar_eur)   AS revpar,
    MEASURE(avg_adr_eur)            AS adr,
    MEASURE(avg_nps_score)          AS nps
  FROM {view_fqn}
"""))
