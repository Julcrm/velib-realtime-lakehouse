from dagster import Definitions, load_assets_from_modules, EnvVar, ScheduleDefinition
from src.assets import bronze, silver, station_reference, velib_alerte, maintenance
from src.resources import MinioResource, SparkIO

# Chargement auto
all_assets = load_assets_from_modules([bronze, silver, station_reference, velib_alerte, maintenance])

# 1. Ingestion
ingestion_schedule = ScheduleDefinition(
    name="ingestion_redpanda_5m",
    cron_schedule="*/5 * * * *",
    target=bronze.velib_redpanda_producer,
)

# 2. Processing
processing_schedule = ScheduleDefinition(
    name="processing_silver_gold_15m",
    cron_schedule="*/15 * * * *",
    target=[silver.velib_stats_silver, velib_alerte.velib_critical_alerts],
)

# 3. Maintenance
daily_schedule = ScheduleDefinition(
    name="daily_maintenance_and_reference",
    cron_schedule="0 0 * * *",
    target=[station_reference.velib_reference_bronze, maintenance.bronze_cleanup],
)

defs = Definitions(
    assets=all_assets,
    schedules=[ingestion_schedule, processing_schedule, daily_schedule],
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