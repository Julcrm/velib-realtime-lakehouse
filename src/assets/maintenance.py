import dagster as dg
from datetime import datetime, timedelta, timezone
from src.resources import MinioResource


@dg.asset(
    group_name="maintenance",
    compute_kind="python",
    name="bronze_cleanup",
)
def bronze_cleanup_policy(context, minio: MinioResource) -> dg.MaterializeResult:
    """
    Supprime les fichiers JSON bruts vieux de plus de 3 jours (72h).
    """
    # CONFIGURATION
    RETENTION_HOURS = 72
    # On récupère le nom du bucket dynamiquement depuis la ressource
    BUCKET = minio.bucket_name
    PREFIX = "velib/"

    # Calcul de la date limite pour la suppression
    cutoff_date = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)

    context.log.info(f"Cible : Bucket '{BUCKET}', Préfixe '{PREFIX}'")
    context.log.info(f"Suppression de tout fichier antérieur à : {cutoff_date}")

    # On récupère le client
    s3 = minio.get_client()


    paginator = s3.get_paginator("list_objects_v2")

    deleted_count = 0
    reclaimed_bytes = 0

    # Parcourt tout le bucket
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            if obj["LastModified"] < cutoff_date:
                s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
                deleted_count += 1
                reclaimed_bytes += obj["Size"]

    # LOGGING & MÉTRIQUES
    size_mb = reclaimed_bytes / (1024 * 1024)

    if deleted_count > 0:
        msg = f"Nettoyage effectué : {deleted_count} fichiers supprimés ({size_mb:.2f} MB libérés)."
        context.log.info(msg)
    else:
        context.log.info("Rien à nettoyer, aucun fichier n'était éligible à la suppression.")

    return dg.MaterializeResult(
        metadata={
            "deleted_files": deleted_count,
            "reclaimed_mb": size_mb,
            "retention_hours": RETENTION_HOURS,
            "bucket": BUCKET
        }
    )