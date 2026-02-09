"""
Assets de génération d'alertes pour le pipeline de données Vélib.

Ce module génère des alertes critiques pour le dashboard opérationnel en se basant sur
l'analyse des statistiques de la couche Silver (ex: stations vides avec tendance négative).
"""

import dagster as dg
from pyspark.sql import functions as F
from src.resources import SparkIO


@dg.asset(
    group_name="operations",
    compute_kind="spark",
    name="velib_critical_alerts",
    deps=["velib_stats_history_silver"]  # Dépend du Silver
)
def velib_critical_alerts(context, spark_io: SparkIO) -> dg.MaterializeResult:
    """
    Génère la liste des stations en alerte pour le Dashboard Ops.

    Règle : Moins de 3 vélos ET une perte nette récente (Vidage actif).

    Args:
        context: Contexte Dagster.
        spark_io: Ressource Spark.

    Retourne:
        MaterializeResult avec les statistiques d'alerte.
    """
    spark = spark_io.get_session("VelibAlerts")

    # 1. LECTURE Silver (Optimisé par partition date)
    # Lit uniquement la date d'aujourd'hui. context.run_date pourrait être utilisé, mais current_date standard fonctionne ici.
    # Spark gère le "current_date" selon l'heure machine.
    df_stats = spark.read.parquet("s3a://silver/velib_stats") \
        .filter(F.col("date") == F.current_date())

    # 2. APPLICATION des Règles Métier
    # On cherche le DERNIER état connu de chaque station.
    # On groupe par station et on prend le max du timestamp.
    df_latest = df_stats.groupBy("station_code", "station_name").agg(
        F.max("last_reported").alias("last_seen"),
        F.last("bikes_available").alias("bikes"),
        F.last("net_flow").alias("trend"),  # Tendance instantanée
        F.last("moving_avg_1h").alias("avg_1h")
    )

    # 3. FILTRAGE des Alertes
    # "Critique" = Moins de 3 vélos ET Tendance négative (ou nulle si déjà vide).
    df_alerts = df_latest.filter(
        (F.col("bikes") < 3) &
        (F.col("trend") <= 0)
    ).withColumn(
        "alert_level",
        F.when(F.col("bikes") == 0, "CRITICAL_EMPTY")
        .otherwise("WARNING_LOW")
    )

    # 4. ÉCRITURE (Overwrite Single File)
    # Le dashboard React lira ce fichier unique.
    save_path = "s3a://gold/alerts/current_status"

    df_alerts.coalesce(1).write.mode("overwrite").parquet(save_path)

    # 5. MÉTRIQUES pour l'Observabilité
    alert_count = df_alerts.count()
    context.log.info(f"🚨 ALERTES GÉNÉRÉES : {alert_count} stations critiques.")

    return dg.MaterializeResult(
        metadata={
            "path": save_path,
            "alert_count": alert_count,
            "status": "Generated"
        }
    )