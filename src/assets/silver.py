import dagster as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.resources import SparkIO
import src.assets.bronze as bronze

@dg.asset(
    group_name="transformation",
    compute_kind="spark",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    deps=[bronze.velib_redpanda_producer]
)
def velib_stats_streaming_silver(context, spark_io: SparkIO):
    spark = spark_io.get_session("VelibStreamingSilver")

    # 1. DÉFINITION DU SCHÉMA (Crucial pour le streaming)
    schema = StructType([
        StructField("stationcode", StringType()),
        StructField("name", StringType()),
        StructField("numdocksavailable", IntegerType()),
        StructField("numbikesavailable", IntegerType()),
        StructField("mechanical", IntegerType()),
        StructField("ebike", IntegerType()),
        StructField("duedate", StringType())
    ])

    # 2. LECTURE DU FLUX REDPANDA
    # On lit depuis le topic que ton producer remplit
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "redpanda:9092")
        .option("subscribe", "velib.raw.status")
        .option("startingOffsets", "earliest") # On reprend tout au début la première fois
        .load()
    )

    # 3. PARSING & TRANSFORMATION
    # Spark reçoit du binaire, on le transforme en colonnes utilisables
    df_parsed = df_raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Conversion des dates et préparation des types
    df_transformed = df_parsed.withColumn(
        "last_reported", F.to_timestamp(F.col("duedate"))
    ).withColumn(
        "date", F.to_date(F.col("last_reported"))
    )

    # --- 4. ÉCRITURE EN STREAMING (Sink) ---
    target_path = "s3a://silver/velib_stats"
    checkpoint_path = "s3a://silver/checkpoints/velib_stats"

    query = (
        df_transformed.writeStream
        .format("parquet")
        .option("path", target_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("date")
        .outputMode("append")
        # MODIFICATION : Spark traite tout ce qui est disponible dans Redpanda et s'arrête
        .trigger(availableNow=True)
        .start()
    )

    context.log.info(f"🚀 Traitement du flux vers {target_path} démarré...")

    # MODIFICATION : On attend la fin réelle du traitement (nécessaire pour availableNow)
    query.awaitTermination()

    # On peut optionnellement récupérer des statistiques sur ce qui a été traité
    last_progress = query.lastProgress
    num_input_rows = last_progress['numInputRows'] if last_progress else 0

    return dg.MaterializeResult(
        metadata={
            "sink_path": target_path,
            "mode": "Micro-batch (AvailableNow)",
            "rows_processed": num_input_rows,
            "format": "Parquet"
        }
    )