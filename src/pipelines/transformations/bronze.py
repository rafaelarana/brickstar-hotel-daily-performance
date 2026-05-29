"""Bronze layer · Auto Loader from RAW Volume.

The mock SQL Server tables (`hotels`, `reservations`, `control_endpoints`) are
already in Bronze (seeded by `00_seed_mock_sources.py`) — Silver reads them
directly. This file only handles the HTTP RAW Volume → Bronze stream.
"""

import dlt
from pyspark.sql import functions as F


CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")
VOLUME = spark.conf.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


@dlt.table(
    name="bronze_web_metrics",
    comment="Daily web-service metrics ingested via Auto Loader from RAW Volume",
    cluster_by=["hotel_id", "metric_date"],
)
def bronze_web_metrics():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(VOLUME_PATH)
        .withColumn("_ingested_ts", F.current_timestamp())
        .withColumn("_source", F.lit("mock_http_fanout"))
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("metric_date", F.to_date("metric_date"))
    )
