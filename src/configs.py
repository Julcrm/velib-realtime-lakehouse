"""
Définitions de configuration pour le pipeline de données Vélib.

Ce module définit les schémas de configuration utilisant le système Config de Dagster.
"""

from dagster import Config


class VelibApiConfig(Config):
    """
    Configuration pour la connexion à l'API Vélib.

    Attributs:
        url (str): L'URL du endpoint pour le statut des stations.
        timeout_seconds (int): Timeout pour les requêtes API en secondes.
        station_info_url (str): L'URL du endpoint pour les informations des stations (données de référence).
    """
    url: str = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    timeout_seconds: int = 10
    station_info_url: str = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
