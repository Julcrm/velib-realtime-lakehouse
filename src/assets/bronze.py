"""
Assets de la couche Bronze pour le pipeline de données Vélib.

Ce module contient les assets responsables de l'ingestion des données brutes depuis l'API Vélib
vers la couche Bronze (MinIO).
"""

import datetime
from datetime import datetime
import dagster as dg
import requests

from src.resources import MinioResource
from src.configs import VelibApiConfig


# Définition de l'Asset Dagster.
# 'key_prefix' organise l'asset logiquement dans l'interface Dagster.
# 'compute_kind="python"' indique que le traitement est effectué par le CPU local.
@dg.asset(
    group_name="ingestion",
    compute_kind="python",
    key_prefix=["velib", "bronze"]
)
def velib_realtime_bronze(context, config: VelibApiConfig, minio: MinioResource) -> dg.MaterializeResult:
    """
    Ingère les données temps réel depuis l'API Vélib.

    Récupère le JSON brut, valide la structure minimale, et stocke le résultat
    dans la couche Bronze (Raw) du Data Lake sur MinIO.

    Args:
        context: Contexte Dagster.
        config: Configuration pour l'API Vélib.
        minio: Ressource MinIO pour le stockage de fichiers.

    Retourne:
        MaterializeResult avec des métadonnées sur l'ingestion.
    """
    context.log.info(f"Démarrage ingestion API : {config.url}")

    try:
        # Appel HTTP avec timeout explicite via la configuration pour éviter les blocages infinis.
        response = requests.get(config.url, timeout=config.timeout_seconds)

        # Lève une HTTPError si le code statut est 4xx ou 5xx.
        # Cela permet à Dagster de marquer l'exécution comme "Failed" et de déclencher les retries.
        response.raise_for_status()

        # Parsing JSON immédiat pour valider que la réponse n'est pas une erreur HTML ou un binaire corrompu.
        payload = response.json()

        # Extraction défensive des données :
        # Utilisation de .get() chaînés pour éviter une KeyError si le schéma de l'API change.
        # Permet de calculer un KPI métier réel.
        stations = payload.get("data", {}).get("stations", [])
        record_count = len(stations)

        # Alerte technique (Warning) si l'API répond correctement (HTTP 200) mais envoie des données vides.
        if record_count == 0:
            context.log.warn("API valide mais 0 stations trouvées !")

    except Exception as e:
        # Propagation de l'erreur pour que l'orchestrateur (Dagster) gère le cycle de vie de l'échec.
        raise e

    # Stratégie de Partitionnement.
    # Découpage hiérarchique : Année > Mois > Jour > Heure.
    # Objectif : Optimiser lors des lectures futures par Spark ou DuckDB.
    now = datetime.now()
    partition_path = now.strftime("year=%Y/month=%m/day=%d/hour=%H")

    # Génération d'un nom de fichier unique avec timestamp précis (Minutes/Secondes).
    # Chaque run crée un nouveau fichier, pas d'écrasement accidentel.
    filename = now.strftime("velib_status_%M%S.json")
    object_key = f"velib/{partition_path}/{filename}"

    # Délégation de l'écriture à la ressource MinIO.
    # Sépare la logique métier (extraction/partitionnement) de la logique d'infrastructure (connexion S3/MinIO).
    s3_path = minio.upload_json(object_key, payload)

    context.log.info(f"Sauvegardé : {s3_path} ({record_count} stations)")

    # Retour d'un résultat enrichi.
    # Expose des métadonnées critiques directement dans l'UI de Dagster pour l'observabilité :
    # - record_count : Volume de données métier.
    # - api_latency_ms : Performance du service tiers.
    # - partition : Traçabilité du stockage.
    return dg.MaterializeResult(
        metadata={
            "s3_path": s3_path,
            "record_count": record_count,
            "api_latency_ms": response.elapsed.total_seconds() * 1000,
            "partition": partition_path
        }
    )