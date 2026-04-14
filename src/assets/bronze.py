import json
import requests
import dagster as dg
from kafka import KafkaProducer
from src.configs import VelibApiConfig

@dg.asset(
    group_name="ingestion",
    compute_kind="python",
    name="velib_redpanda_producer"
)
def velib_redpanda_producer(context, config: VelibApiConfig) -> dg.MaterializeResult:
    """
    Récupère les données de l'API et les injecte dans le cluster Redpanda.
    """

    # 1. Connexion à Redpanda (utilise l'adresse du service dans Docker)
    try:
        producer = KafkaProducer(
            bootstrap_servers=['redpanda:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1 # Plus rapide pour du streaming de capteurs
        )
    except Exception as e:
        context.log.error(f"Échec connexion Redpanda : {e}")
        raise e

    # 2. Configuration API "Ninja" pour éviter le ban
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    params = {
        "select": "stationcode,name,numdocksavailable,numbikesavailable,mechanical,ebike,duedate",
        "limit": -1
    }

    # 3. Récupération des données
    context.log.info("Appel API Velib (Mode Export)...")
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    stations = response.json()

    # 4. Envoi vers Redpanda
    # On envoie chaque station comme un message individuel
    for station in stations:
        producer.send(
            topic='velib.raw.status',
            key=str(station['stationcode']).encode('utf-8'),
            value=station
        )

    producer.flush() # On attend que tout soit envoyé
    producer.close()

    context.log.info(f"✅ {len(stations)} messages envoyés dans Redpanda (topic: velib.raw.status)")

    return dg.MaterializeResult(
        metadata={
            "count": len(stations),
            "topic": "velib.raw.status"
        }
    )