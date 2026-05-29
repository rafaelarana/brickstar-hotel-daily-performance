"""Silver layer · clean, dedup, enforce expectations, join hotels."""

import dlt
from pyspark.sql import functions as F


CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")


@dlt.table(
    name="silver_hotels",
    comment="Cleaned hotel master with referential checks",
    cluster_by=["hotel_id"],
)
@dlt.expect_or_fail("hotel_id_not_null", "hotel_id IS NOT NULL")
@dlt.expect("country_set", "country IS NOT NULL")
@dlt.expect("rooms_positive", "rooms > 0")
def silver_hotels():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.hotels")
        .dropDuplicates(["hotel_id"])
        .withColumn("hotel_name", F.trim("hotel_name"))
    )


@dlt.table(
    name="silver_reservations",
    comment="Reservations with status normalisation, cancellations filtered, joined with hotel master",
    cluster_by=["hotel_id", "check_in"],
)
@dlt.expect_or_drop("valid_dates", "check_in <= check_out")
@dlt.expect_or_drop("positive_revenue", "total_revenue_eur >= 0")
@dlt.expect("known_channel", "channel IN ('DIRECT','BOOKING','EXPEDIA','HOTELBEDS','TUI','TRAVEL_AGENT')")
def silver_reservations():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.reservations")
        .where("status <> 'CANCELLED'")
        .dropDuplicates(["reservation_id"])
        .withColumn("check_in",  F.to_date("check_in"))
        .withColumn("check_out", F.to_date("check_out"))
    )


@dlt.table(
    name="silver_daily_web_metrics",
    comment="Latest version of each daily-metrics payload per (hotel × date)",
    cluster_by=["hotel_id", "metric_date"],
)
@dlt.expect_or_drop("hotel_id_set", "hotel_id IS NOT NULL")
@dlt.expect_or_drop("metric_date_set", "metric_date IS NOT NULL")
@dlt.expect("occupancy_range", "occupancy_rate BETWEEN 0 AND 1")
def silver_daily_web_metrics():
    return (
        dlt.read_stream("bronze_web_metrics")
        .withWatermark("_ingested_ts", "1 hour")
        .dropDuplicates(["hotel_id", "metric_date"])
    )
