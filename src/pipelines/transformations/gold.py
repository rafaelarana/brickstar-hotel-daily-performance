"""Gold layer · hotel daily KPIs.

Aggregates Silver reservations + web metrics into a single fact per
(hotel × date) with the KPIs Brickstar discussed in the meeting:
RevPAR, ADR, occupancy %, daily revenue, NPS, cancellations.
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_hotel_daily_kpis",
    comment="Brickstar hotel × date KPIs · RevPAR, ADR, occupancy, NPS, GOP proxy. Consumed by Metric Views, AI/BI, Genie.",
    cluster_by=["hotel_id", "metric_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect_or_drop("revpar_non_neg", "revpar_eur >= 0")
@dlt.expect_or_drop("occupancy_pct_range", "occupancy_pct BETWEEN 0 AND 100")
def gold_hotel_daily_kpis():
    hotels = dlt.read("silver_hotels")
    metrics = dlt.read("silver_daily_web_metrics")

    # Daily reservation revenue per hotel × check_in date — proxy for booked revenue
    res = (
        dlt.read("silver_reservations")
        .groupBy("hotel_id", F.col("check_in").alias("metric_date"))
        .agg(
            F.sum("total_revenue_eur").alias("booked_revenue_eur"),
            F.countDistinct("reservation_id").alias("reservations_count"),
            F.avg("adr_eur").alias("avg_booking_adr_eur"),
        )
    )

    out = (
        metrics.alias("m")
        .join(hotels.alias("h"), "hotel_id", "left")
        .join(res.alias("r"), ["hotel_id", "metric_date"], "left")
        .select(
            F.col("m.hotel_id"),
            F.col("m.metric_date"),
            F.col("h.hotel_name"),
            F.col("h.country"),
            F.col("h.destination"),
            F.col("h.market_segment"),
            F.col("h.stars"),
            F.col("h.rooms").alias("rooms_total"),
            F.col("m.rooms_occupied"),
            (F.col("m.occupancy_rate") * 100).alias("occupancy_pct"),
            F.col("m.adr_eur"),
            F.col("m.daily_revenue_eur"),
            # RevPAR = revenue / available rooms
            F.round(F.col("m.daily_revenue_eur") / F.col("h.rooms"), 2).alias("revpar_eur"),
            F.col("m.walkins"),
            F.col("m.cancellations"),
            F.col("m.nps_responses"),
            F.col("m.nps_score"),
            F.coalesce(F.col("r.booked_revenue_eur"), F.lit(0.0)).alias("booked_revenue_eur"),
            F.coalesce(F.col("r.reservations_count"), F.lit(0)).alias("reservations_count"),
            # Rough GOP proxy: 40-65% margin on daily revenue depending on market
            F.round(
                F.col("m.daily_revenue_eur")
                * F.when(F.col("h.market_segment") == "Beachfront Luxury", 0.62)
                .when(F.col("h.market_segment") == "All-Inclusive", 0.58)
                .when(F.col("h.market_segment") == "Family Resort", 0.52)
                .when(F.col("h.market_segment") == "Urban", 0.46)
                .when(F.col("h.market_segment") == "Wellness", 0.55)
                .otherwise(0.50),
                2,
            ).alias("gop_proxy_eur"),
        )
    )
    return out
