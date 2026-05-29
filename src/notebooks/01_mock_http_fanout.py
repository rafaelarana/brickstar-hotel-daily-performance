# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · HTTP fan-out simulator → RAW Volume
# MAGIC
# MAGIC Replaces the real Lakeflow Job notebook that orchestrates ~50 HTTPS calls
# MAGIC per hotel. Instead of actually hitting an external web service, this
# MAGIC notebook:
# MAGIC
# MAGIC 1. Reads the mocked control table (`control_endpoints`).
# MAGIC 2. For each active hotel, synthesises a daily-metrics JSON payload as if
# MAGIC    returned by the source web service.
# MAGIC 3. Writes one JSON file per (hotel, date) into the UC Volume — Auto Loader
# MAGIC    will pick them up downstream.

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_89j9qf")
dbutils.widgets.text("schema_bronze", "brickstar_bronze")
dbutils.widgets.text("volume_name", "raw_http")
dbutils.widgets.text("history_days", "60")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
volume_name = dbutils.widgets.get("volume_name")
history_days = int(dbutils.widgets.get("history_days"))

volume_path = f"/Volumes/{catalog}/{schema_bronze}/{volume_name}"
print(f"Writing JSON payloads to {volume_path}")

# COMMAND ----------

import json
import os
import random
from datetime import date, timedelta

random.seed(43)

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema_bronze}")

endpoints = spark.table("control_endpoints").where("active = true").collect()
hotels = spark.table("hotels").collect()
hotel_meta = {h.hotel_id: (h.rooms, h.market_segment, h.country) for h in hotels}

print(f"Active endpoints: {len(endpoints)} hotels · {history_days} days each = "
      f"{len(endpoints) * history_days} payloads")

# COMMAND ----------

def simulate_daily_payload(hotel_id: str, day: date) -> dict:
    rooms, market, country = hotel_meta[hotel_id]
    weekend_boost = 1.10 if day.weekday() >= 5 else 1.0
    base_occ = {
        "Beachfront Luxury": 0.74,
        "All-Inclusive":     0.70,
        "Family Resort":     0.66,
        "Urban":             0.55,
        "Wellness":          0.62,
    }.get(market, 0.65)
    occupancy_rate = min(0.98, max(0.20, base_occ * weekend_boost * random.uniform(0.85, 1.15)))
    rooms_occupied = int(rooms * occupancy_rate)
    adr = random.uniform(120, 310)
    daily_revenue = round(rooms_occupied * adr, 2)
    walkins = random.randint(0, 15)
    cancellations = random.randint(0, max(1, rooms_occupied // 12))
    nps_resp = random.randint(20, 80)
    nps_score = round(random.uniform(40, 78), 1)

    return {
        "hotel_id":        hotel_id,
        "metric_date":     day.isoformat(),
        "rooms_total":     rooms,
        "rooms_occupied":  rooms_occupied,
        "occupancy_rate":  round(occupancy_rate, 4),
        "adr_eur":         round(adr, 2),
        "daily_revenue_eur": daily_revenue,
        "walkins":         walkins,
        "cancellations":   cancellations,
        "nps_responses":   nps_resp,
        "nps_score":       nps_score,
        "source_endpoint": f"https://mock-brickstar-api.example/v1/hotels/{hotel_id}/daily_metrics",
        "ingested_at":     None,  # filled by Auto Loader / SDP
    }

# COMMAND ----------

# Write one JSON per (hotel, date). Use UC Volume; dbutils.fs.put handles it.
today = date.today()
written = 0
for ep in endpoints:
    hotel_id = ep.hotel_id
    for d in range(history_days):
        day = today - timedelta(days=history_days - d)
        payload = simulate_daily_payload(hotel_id, day)
        # partitioned-style folder layout (Auto Loader handles this fine)
        rel = f"hotel_id={hotel_id}/year={day.year}/month={day.month:02d}/day={day.day:02d}/metrics.json"
        full = f"{volume_path}/{rel}"
        dbutils.fs.put(full, json.dumps(payload), overwrite=True)
        written += 1

print(f"✓ wrote {written:,} JSON files to {volume_path}")

# COMMAND ----------

# Sanity check: count top-level hotel partitions + sample a few payloads.
top = dbutils.fs.ls(volume_path)
print(f"Top-level hotel partitions: {len(top)}")
sample = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(volume_path)
    .limit(20)
)
display(sample)
