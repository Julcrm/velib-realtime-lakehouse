"""
Assets de la couche Silver pour le pipeline de données Vélib.
"""

import dagster as dg
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, LongType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from src.resources import SparkIO


@dg.asset(
    group_name="transformation",
    compute_kind="spark",
    name="velib_stats_history_silver",
    deps=["velib_realtime_bronze", "velib_reference_bronze"]
)
def velib_stats_silver(context, spark_io: SparkIO) -> dg.MaterializeResult:
    spark = spark_io.get_session("VelibSilverOps")

    # --- 1. OPTIMISATION ---
    paths_to_read = []
    base_path = "s3a://bronze/velib"

    now = datetime.now()

    current_month = now.strftime(f"{base_path}/year=%Y/month=%m")
    paths_to_read.append(current_month)

    # Mois précédent
    last_month = now.replace(day=1) - timedelta(days=1)
    prev_month = last_month.strftime(f"{base_path}/year=%Y/month=%m")
    paths_to_read.append(prev_month)

    context.log.info(f"Lecture Récursive des Mois : {paths_to_read}")

    try:
        df_status_raw = spark.read \
            .format("json") \
            .option("recursiveFileLookup", "true") \
            .option("pathGlobFilter", "*.json") \
            .load(paths_to_read)

        # On filtre pour ne garder que les données récentes (3 jours)
        df_status_raw = df_status_raw.filter(
            F.to_date(F.col("last_reported")) >= F.date_sub(F.current_date(), 3)
        )

    except Exception as e:
        if "Path does not exist" in str(e) or "AnalysisException" in str(e):
            context.log.warn(f"Aucun dossier de mois trouvé : {paths_to_read}")
            return dg.MaterializeResult(metadata={"status": "Skipped_NoData"})
        raise e

    # B. Données de Référence
    try:
        df_info_raw = spark.read.json("s3a://bronze/velib/reference/station_information.json")
    except Exception:
        raise Exception("Référentiel Stations Manquant.")

    # --- 3. PRÉPARATION & NETTOYAGE  ---
    df_names = df_info_raw.select(F.explode(F.col("data.stations")).alias("info")).select(
        F.col("info.station_id").cast(LongType()).alias("ref_id"),
        F.col("info.name").alias("station_name")
    )

    df_status = df_status_raw.select(F.explode(F.col("data.stations")).alias("status")).select(
        F.col("status.station_id").cast(LongType()).alias("status_id"),
        F.col("status.stationCode").alias("station_code"),
        F.col("status.num_bikes_available").cast(IntegerType()).alias("bikes_available"),
        F.col("status.num_docks_available").cast(IntegerType()).alias("docks_available"),
        F.col("status.last_reported").cast(LongType()).alias("last_reported_sec"),
        F.from_unixtime(F.col("status.last_reported")).cast(TimestampType()).alias("last_reported")
    )

    # Filtre Zombie, on ne garde que les stations dont le dernier rapport est récent (moins de 24h).
    df_status = df_status.filter(
        F.col("last_reported") >= F.date_sub(F.current_timestamp(), 1)
    )

    df_enriched = df_status.join(F.broadcast(df_names), df_status["status_id"] == df_names["ref_id"], "left")

    # --- 4. CALCULS OPS ---
    window_spec = Window.partitionBy("station_code").orderBy("last_reported_sec")
    window_1h = window_spec.rangeBetween(-3600, 0)

    df_final = df_enriched.withColumn(
        "moving_avg_1h", F.avg("bikes_available").over(window_1h)
    ).withColumn(
        "prev_bikes", F.lag("bikes_available").over(window_spec)
    ).withColumn(
        "net_flow",
        F.col("bikes_available") - F.coalesce(F.col("prev_bikes"), F.col("bikes_available"))
    ).withColumn(
        "date", F.to_date("last_reported")
    ).select(
        "station_code", "station_name", "bikes_available", "docks_available",
        "net_flow", "moving_avg_1h", "last_reported", "date"
    )

    # --- 5. ÉCRITURE ---
    save_path = "s3a://silver/velib_stats"
    df_final.write.mode("overwrite").partitionBy("date").parquet(save_path)

    return dg.MaterializeResult(
        metadata={
            "path": save_path,
            "rows": df_final.count(),
            "input_partitions": len(paths_to_read)
        }
    )