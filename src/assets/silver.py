"""
Assets de la couche Silver.
"""

import dagster as dg
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, LongType
from pyspark.sql.window import Window
from src.resources import SparkIO, MinioResource

@dg.asset(
    group_name="transformation",
    compute_kind="spark",
    name="velib_stats_history_silver",
    deps=["velib_realtime_bronze", "velib_reference_bronze"]
)
def velib_stats_silver(context, spark_io: SparkIO, minio: MinioResource) -> dg.MaterializeResult:
    spark = spark_io.get_session("VelibSilverOps")

    # --- 1. LISTING ---
    # On utilise Boto3 pour lister tous les fichiers JSON présents.

    s3 = minio.get_client()
    bucket = "bronze"
    prefix = "velib/"

    found_files = []

    # Pagination
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    context.log.info("Scan complet du Data Lake (Bronze)...")

    for page in pages:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            if key.endswith(".json") and "reference" not in key:
                full_path = f"s3a://{bucket}/{key}"
                found_files.append(full_path)

    # --- 2. SÉCURITÉ ---
    if not found_files:
        context.log.warn("Aucun fichier JSON trouvé. Le pipeline s'arrête là.")
        return dg.MaterializeResult(metadata={"status": "Skipped_Empty"})

    count = len(found_files)
    context.log.info(f"{count} fichiers identifiés à traiter.")

    files_to_process = found_files

    # --- 3. LECTURE ---
    try:
        df_status_raw = spark.read.json(files_to_process)
    except Exception as e:
        context.log.error(f"Erreur critique Spark : {e}")
        raise e

    # --- 4. DATA REFERENCE ---
    try:
        df_info_raw = spark.read.json(f"s3a://{bucket}/velib/reference/station_information.json")
    except Exception:
        raise Exception("Référentiel Stations Manquant (lance l'asset Reference Bronze !)")

    # --- 5. TRANSFORMATION (Logique Métier) ---

    # a. Explode Reference
    df_names = df_info_raw.select(F.explode(F.col("data.stations")).alias("info")).select(
        F.col("info.station_id").cast(LongType()).alias("ref_id"),
        F.col("info.name").alias("station_name")
    )

    # b. Explode Status
    df_status = df_status_raw.select(F.explode(F.col("data.stations")).alias("status")).select(
        F.col("status.station_id").cast(LongType()).alias("status_id"),
        F.col("status.stationCode").alias("station_code"),
        F.col("status.num_bikes_available").cast(IntegerType()).alias("bikes_available"),
        F.col("status.num_docks_available").cast(IntegerType()).alias("docks_available"),
        F.col("status.last_reported").cast(LongType()).alias("last_reported_sec"),
        F.from_unixtime(F.col("status.last_reported")).cast(TimestampType()).alias("last_reported")
    )

    # c. Nettoyage Zombies (Stations muettes > 24h)
    df_status = df_status.filter(
        F.col("last_reported") >= F.date_sub(F.current_timestamp(), 1)
    )

    # d. Enrichissement
    df_enriched = df_status.join(F.broadcast(df_names), df_status["status_id"] == df_names["ref_id"], "left")

    # e. Fenêtrage (Stats Glissantes)
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

    # --- 6. ÉCRITURE ---
    save_path = "s3a://silver/velib_stats"

    if df_final.head(1):
        df_final.write.mode("overwrite").partitionBy("date").parquet(save_path)
        final_count = df_final.count()
        context.log.info(f"Silver généré : {final_count} lignes insérées.")
    else:
        context.log.warn("Pipeline terminé mais résultat vide (données trop anciennes ?).")
        final_count = 0

    return dg.MaterializeResult(
        metadata={
            "path": save_path,
            "input_files": count,
            "output_rows": final_count
        }
    )