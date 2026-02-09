"""
Assets de génération d'alertes pour le pipeline de données Vélib.
Version corrigée : Gestion robuste du changement de jour (Midnight-Safe).
"""

import dagster as dg
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from src.resources import SparkIO


@dg.asset(
    group_name="operations",
    compute_kind="spark",
    name="velib_critical_alerts",
    deps=["velib_stats_history_silver"]
)
def velib_critical_alerts(context, spark_io: SparkIO) -> dg.MaterializeResult:
    spark = spark_io.get_session("VelibAlerts")

    # --- 1. LECTURE INTELLIGENTE ---
    paths_to_read = []
    base_path = "s3a://silver/velib_stats"

    # On génère les chemins pour Aujourd'hui et Hier
    for i in range(2):
        d = datetime.now() - timedelta(days=i)
        paths_to_read.append(d.strftime(f"{base_path}/date=%Y-%m-%d"))

    context.log.info(f"Scan des partitions Alertes : {paths_to_read}")

    try:
        df_stats = spark.read.option("basePath", base_path).parquet(*paths_to_read)
    except Exception:
        context.log.warn("Aucune donnée Silver trouvée (Alertes ignorées).")
        return dg.MaterializeResult(metadata={"status": "Skipped"})

    # --- 2. FILTRAGE TEMPOREL ---
    df_recent = df_stats.filter(
        F.col("last_reported") >= F.expr("now() - interval 4 hours")
    )

    # --- 3. DÉDUPLICATION ---
    window_spec = Window.partitionBy("station_code").orderBy(F.col("last_reported").desc())

    df_latest = df_recent.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .drop("rank") \
        .select(
            F.col("station_code"),
            F.col("station_name"),
            F.col("bikes_available").alias("bikes"),
            F.col("net_flow").alias("trend"),
            F.col("moving_avg_1h").alias("avg_1h"),
            F.col("last_reported")
        )

    # --- 4. RÈGLES MÉTIER ---
    df_alerts = df_latest.filter(
        (F.col("bikes") < 3) &
        (F.col("trend") <= 0)
    ).withColumn(
        "alert_level",
        F.when(F.col("bikes") == 0, "CRITICAL_EMPTY")
        .otherwise("WARNING_LOW")
    )

    # --- 5. ÉCRITURE ---
    save_path = "s3a://gold/alerts/current_status"

    df_alerts.coalesce(1).write.mode("overwrite").parquet(save_path)

    alert_count = df_alerts.count()
    context.log.info(f"{alert_count} stations en alerte (basé sur l'état temps réel).")

    return dg.MaterializeResult(
        metadata={
            "path": save_path,
            "alert_count": alert_count
        }
    )