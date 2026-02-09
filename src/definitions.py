"""
Définitions Dagster.

Ce module agrège tous les assets, ressources, schedules et sensors
pour définir le code location Dagster.
"""

from dagster import Definitions, load_assets_from_modules, EnvVar, ScheduleDefinition

# Import des modules d'assets (plutôt que les assets individuels)
from src.assets import bronze, silver, station_reference, velib_alerte, maintenance

# Import des ressources
from src.resources import MinioResource
from src.resources import SparkIO


# Chargement automatique de TOUS les assets détectés dans ces modules
all_assets = load_assets_from_modules([bronze, silver, station_reference, velib_alerte, maintenance])

# Schedule haute fréquence (Toutes les 2 minutes) pour les données temps réel
fast_schedule = ScheduleDefinition(
    name="refresh_every_2_min",
    cron_schedule="*/2 * * * *",
    target=[
        bronze.velib_realtime_bronze,
        silver.velib_stats_silver,
        velib_alerte.velib_critical_alerts
    ],
)

# Schedule quotidien (Minuit) : Mise à jour référentiel + Nettoyage
daily_schedule = ScheduleDefinition(
    name="daily_maintenance_and_reference",
    cron_schedule="0 0 * * *",
    target=[
        station_reference.velib_reference_bronze,
        maintenance.bronze_cleanup
    ],
)

# Définitions Globales
defs = Definitions(
    assets=all_assets,
    schedules=[fast_schedule, daily_schedule],
    resources={
        "minio": MinioResource(
            endpoint_url=EnvVar("S3_ENDPOINT_URL"),
            access_key=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            bucket_name="bronze"
        ),
        "spark_io": SparkIO(),
    }
)

