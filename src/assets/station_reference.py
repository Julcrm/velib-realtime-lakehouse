"""
Assets de référence des stations pour le pipeline de données Vélib.

Ce module gère l'ingestion des données de référence (noms des stations, capacités) qui changent peu,
vers la couche Bronze.
"""

import dagster as dg
import requests

from src.resources import MinioResource
from src.configs import VelibApiConfig


@dg.asset(
    group_name="ingestion",
    compute_kind="python",
    name="velib_reference_bronze",
    # Récupéré une fois par jour ou au démarrage, suffisant.
)
def velib_reference_bronze(context, config: VelibApiConfig, minio: MinioResource) -> dg.MaterializeResult:
    """
    Ingère les données de référence des stations Vélib (noms, localisations).

    Récupère le JSON 'station_information' et le téléverse dans la couche Bronze.
    Ce sont des données de référence utilisées pour enrichir les données de statut temps réel.

    Args:
        context: Contexte Dagster.
        config: Configuration de l'API.
        minio: Ressource MinIO.

    Retourne:
        MaterializeResult avec métadonnées.
    """
    url = config.station_info_url
    context.log.info(f"Récupération des données de référence stations : {url}")

    try:
        response = requests.get(url, timeout=config.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        stations = payload.get("data", {}).get("stations", [])
        count = len(stations)

        if count == 0:
            raise ValueError("Données de référence vides !")

    except Exception as e:
        raise e

    # Stockage dans un dossier "reference".
    # Écrase le fichier à chaque fois car c'est une table de dimension (source de vérité).
    object_key = "velib/reference/station_information.json"

    s3_path = minio.upload_json(object_key, payload)

    context.log.info(f"Référence mise à jour : {count} stations.")

    return dg.MaterializeResult(
        metadata={
            "path": s3_path,
            "station_count": count,
            "type": "Names"
        }
    )