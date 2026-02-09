"""
Assets de la couche Silver pour le pipeline de données Vélib.

Ce module traite les données brutes de la couche Bronze (MinIO) pour les nettoyer, les enrichir
et calculer des statistiques dans la couche Silver, prêtes pour l'analytique.
"""

import dagster as dg
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, LongType
from pyspark.sql.window import Window

from src.resources import SparkIO


@dg.asset(
    group_name="transformation",
    compute_kind="spark",
    name="velib_stats_history_silver",
    deps=["velib_realtime_bronze", "velib_reference_bronze"]
)
def velib_stats_silver(context, spark_io: SparkIO) -> dg.MaterializeResult:
    """
    Transforme et enrichit les données des stations Vélib.

    Lit les données brutes du Bronze (MinIO), filtre, nettoie, calcule des métriques opérationnelles
    (flux net, moyenne mobile), et écrit dans la couche Silver (MinIO).

    Args:
        context: Contexte Dagster.
        spark_io: Ressource Spark pour la gestion de session.

    Retourne:
        MaterializeResult avec les métadonnées de traitement.
    """
    spark = spark_io.get_session("VelibSilverOps")

    # 1. LECTURE (Utilisation du wildcard year=* pour éviter les erreurs de dossier)
    try:
        df_status_raw = spark.read.option("basePath", "s3a://bronze/velib").json("s3a://bronze/velib/year=*")

        # Filtre 1 : Garder uniquement les FICHIERS RÉCENTS (Optimisation Spark)
        # Évite le listing extensif des anciennes partitions.
        df_status_raw = df_status_raw.filter(
            F.to_timestamp(F.col("lastUpdatedOther")) >= F.date_sub(F.current_timestamp(), 2)
        )
    except Exception as e:
        context.log.warn(f"Pas de données Bronze trouvées : {e}")
        return dg.MaterializeResult(metadata={"status": "Skipped"})

    # B. Données de Référence
    try:
        df_info_raw = spark.read.json("s3a://bronze/velib/reference/station_information.json")
    except Exception:
        raise Exception("❌ Référentiel Stations Manquant.")

    # 2. PRÉPARATION & NETTOYAGE

    # Dimension : Noms des Stations
    df_names = df_info_raw.select(F.explode(F.col("data.stations")).alias("info")).select(
        F.col("info.station_id").cast(LongType()).alias("ref_id"),
        F.col("info.name").alias("station_name")
    )

    # Faits : Statut des Stations
    df_status = df_status_raw.select(F.explode(F.col("data.stations")).alias("status")).select(
        F.col("status.station_id").cast(LongType()).alias("status_id"),
        F.col("status.stationCode").alias("station_code"),
        F.col("status.num_bikes_available").cast(IntegerType()).alias("bikes_available"),
        F.col("status.num_docks_available").cast(IntegerType()).alias("docks_available"),
        F.col("status.last_reported").cast(LongType()).alias("last_reported_sec"),
        F.from_unixtime(F.col("status.last_reported")).cast(TimestampType()).alias("last_reported")
    )

    # --- FILTRE ZOMBIE ---
    # Supprime les stations qui n'ont pas communiqué depuis 24h.
    # (Même si le fichier API est récent, les données de la station peuvent être obsolètes).
    df_status = df_status.filter(
        F.col("last_reported") >= F.date_sub(F.current_timestamp(), 1)
    )

    # Jointure Faits avec Dimension
    df_enriched = df_status.join(F.broadcast(df_names), df_status["status_id"] == df_names["ref_id"], "left")

    # 3. CALCULS OPS
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

    # 4. ÉCRITURE
    save_path = "s3a://silver/velib_stats"
    # Écrasement par partition pour assurer l'idempotence au sein de la même journée
    df_final.write.mode("overwrite").partitionBy("date").parquet(save_path)

    return dg.MaterializeResult(metadata={"path": save_path, "rows": df_final.count()})