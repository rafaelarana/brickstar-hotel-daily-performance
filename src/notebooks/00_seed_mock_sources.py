# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Seed mock SQL Server + control endpoints
# MAGIC
# MAGIC Replaces Lakeflow Connect output for SQL Server in the real architecture.
# MAGIC Generates synthetic Brickstar hotel + reservation data into Bronze and
# MAGIC seeds the `control.endpoints` table that mocks Lakebase Postgres for the
# MAGIC HTTP fan-out notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_89j9qf")
dbutils.widgets.text("schema_bronze", "brickstar_bronze")
dbutils.widgets.text("num_hotels", "30")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
num_hotels = int(dbutils.widgets.get("num_hotels"))

print(f"Seeding into {catalog}.{schema_bronze} · num_hotels={num_hotels}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType,
)
import random

random.seed(42)

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema_bronze}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## hotels master (mock SQL Server table)

# COMMAND ----------

destinations = [
    ("Mallorca",  "ES"), ("Ibiza",      "ES"), ("Tenerife",   "ES"),
    ("Lanzarote", "ES"), ("Fuerteventura","ES"), ("Barcelona", "ES"),
    ("Cancun",    "MX"), ("Riviera Maya","MX"), ("Punta Cana","DO"),
    ("Montego Bay","JM"), ("Salvador",  "BR"), ("Cabo Verde","CV"),
    ("Algarve",   "PT"), ("Crete",      "GR"), ("Hammamet",   "TN"),
]
markets = ["Beachfront Luxury", "All-Inclusive", "Family Resort", "Urban", "Wellness"]
star_levels = [4, 4, 4, 5, 5, 5]

rows = []
for i in range(num_hotels):
    dest, country = destinations[i % len(destinations)]
    rows.append({
        "hotel_id":       f"BRX-{1000 + i}",
        "hotel_name":     f"Brickstar {dest} {['Resort', 'Selection', 'Grand', 'Bay'][i % 4]} {i // len(destinations) + 1}",
        "country":        country,
        "destination":    dest,
        "market_segment": markets[i % len(markets)],
        "stars":          star_levels[i % len(star_levels)],
        "rooms":          random.randint(180, 720),
        "active":         True,
    })

hotels_df = spark.createDataFrame(rows)

(hotels_df
    .write.format("delta")
    .mode("overwrite")
    .clusterBy("hotel_id")
    .option("overwriteSchema", "true")
    .saveAsTable("hotels"))

display(spark.table("hotels").limit(10))
print(f"hotels: {spark.table('hotels').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## destinations_geo (reference table for map visualisations)
# MAGIC
# MAGIC Latitude / longitude + ISO codes per destination so the dashboard can
# MAGIC drive a symbol-map widget over the portfolio.

# COMMAND ----------

geo_rows = [
    # (destination,        country_iso2, country_iso3, country_name,        lat,       lon)
    ("Mallorca",           "ES", "ESP", "Spain",                39.6953,    3.0176),
    ("Ibiza",              "ES", "ESP", "Spain",                38.9067,    1.4206),
    ("Tenerife",           "ES", "ESP", "Spain",                28.2916,  -16.6291),
    ("Lanzarote",          "ES", "ESP", "Spain",                29.0469,  -13.5899),
    ("Fuerteventura",      "ES", "ESP", "Spain",                28.3587,  -14.0535),
    ("Barcelona",          "ES", "ESP", "Spain",                41.3851,    2.1734),
    ("Cancun",             "MX", "MEX", "Mexico",               21.1619,  -86.8515),
    ("Riviera Maya",       "MX", "MEX", "Mexico",               20.5083,  -87.0964),
    ("Punta Cana",         "DO", "DOM", "Dominican Republic",   18.5601,  -68.3725),
    ("Montego Bay",        "JM", "JAM", "Jamaica",              18.4762,  -77.8939),
    ("Salvador",           "BR", "BRA", "Brazil",              -12.9714,  -38.5014),
    ("Cabo Verde",         "CV", "CPV", "Cabo Verde",           16.5388,  -23.0418),
    ("Algarve",            "PT", "PRT", "Portugal",             37.0179,   -7.9304),
    ("Crete",              "GR", "GRC", "Greece",               35.2401,   24.8093),
    ("Hammamet",           "TN", "TUN", "Tunisia",              36.4015,   10.6168),
]

geo_df = spark.createDataFrame(
    geo_rows,
    schema="destination string, country_iso2 string, country_iso3 string, country_name string, lat double, lon double",
)

(geo_df
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("destinations_geo"))

display(spark.table("destinations_geo"))
print(f"destinations_geo: {spark.table('destinations_geo').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## reservations (mock SQL Server table · ~60 days history)

# COMMAND ----------

dbutils.widgets.text("history_days", "60")
history_days = int(dbutils.widgets.get("history_days"))

# Generate a (hotel × date) skeleton then expand with reservations density.
hotels = [r.hotel_id for r in spark.table("hotels").select("hotel_id").collect()]
hotel_rooms = {r.hotel_id: r.rooms for r in spark.table("hotels").select("hotel_id", "rooms").collect()}

channels = ["DIRECT", "BOOKING", "EXPEDIA", "HOTELBEDS", "TUI", "TRAVEL_AGENT"]
statuses = ["CONFIRMED", "CHECKED_IN", "CHECKED_OUT", "CANCELLED"]

reservations = []
res_id = 0
for hotel in hotels:
    rooms = hotel_rooms[hotel]
    for d in range(history_days):
        # ~30-90% occupancy/day with weekend bump
        from datetime import date, timedelta
        day = date.today() - timedelta(days=history_days - d)
        weekend_boost = 1.15 if day.weekday() >= 5 else 1.0
        n_res = int(rooms * random.uniform(0.30, 0.85) * weekend_boost)
        for _ in range(n_res):
            res_id += 1
            stay_nights = random.randint(2, 14)
            guests = random.randint(1, 4)
            adr = random.uniform(110, 320)
            channel = random.choice(channels)
            # cancellations bias
            status = "CANCELLED" if random.random() < 0.08 else random.choice(statuses[:3])
            reservations.append({
                "reservation_id": f"RES-{res_id:08d}",
                "hotel_id":       hotel,
                "booking_ts":     (day - timedelta(days=random.randint(0, 45))).strftime("%Y-%m-%d %H:%M:%S"),
                "check_in":       day.strftime("%Y-%m-%d"),
                "check_out":      (day + timedelta(days=stay_nights)).strftime("%Y-%m-%d"),
                "guests":         guests,
                "nights":         stay_nights,
                "channel":        channel,
                "adr_eur":        round(adr, 2),
                "total_revenue_eur": round(adr * stay_nights, 2),
                "status":         status,
            })

res_df = spark.createDataFrame(reservations)

(res_df
    .write.format("delta")
    .mode("overwrite")
    .clusterBy("hotel_id", "check_in")
    .option("overwriteSchema", "true")
    .saveAsTable("reservations"))

print(f"reservations: {spark.table('reservations').count():,} rows")
display(spark.table("reservations").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## control.endpoints (mocks Lakebase Postgres)
# MAGIC
# MAGIC In the real architecture this lives in a Postgres `control` schema and is read
# MAGIC by the HTTP fan-out Notebook. We mock it as a Delta table.

# COMMAND ----------

endpoints_rows = []
for hotel in hotels:
    endpoints_rows.append({
        "hotel_id":      hotel,
        "endpoint_url":  f"https://mock-brickstar-api.example/v1/hotels/{hotel}/daily_metrics",
        "auth_method":   "OAUTH2",
        "params_json":   '{"include":"occupancy,revenue,walkins,cancellations"}',
        "active":        True,
    })

(spark.createDataFrame(endpoints_rows)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("control_endpoints"))

print(f"control_endpoints: {spark.table('control_endpoints').count()} rows")
display(spark.table("control_endpoints").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ✓ Mock SQL Server tables (`hotels`, `reservations`) + control endpoints
# MAGIC ready. Trigger `01_mock_http_fanout` next to populate the RAW Volume.
