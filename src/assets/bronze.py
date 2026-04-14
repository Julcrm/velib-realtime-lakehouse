import json
import requests
import dagster as dg
from kafka import KafkaProducer
from src.configs import VelibApiConfig

@dg.asset(
    group_name="ingestion",
    compute_kind="python",
    name="velib_redpanda_producer",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager()
)
def velib_redpanda_producer(context, config: VelibApiConfig) -> dg.MaterializeResult:
    # 1. Connexion à Redpanda
    try:
        producer = KafkaProducer(
            bootstrap_servers=['redpanda:9092'],
            # On sérialise en JSON pour que Spark puisse le parser avec son schéma
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            # On ajoute un timeout pour ne pas bloquer l'asset si Redpanda est saturé
            request_timeout_ms=5000
        )
    except Exception as e:
        context.log.error(f"Échec connexion Redpanda : {e}")
        raise e

    # 2. Configuration API "Ninja"
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
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        stations = response.json()
    except Exception as e:
        context.log.error(f"Erreur API : {e}")
        raise e

    # 4. Envoi vers Redpanda
    for station in stations:
        producer.send(
            topic='velib.raw.status',
            # Utiliser le code station comme clé est une "Best Practice" Kafka/Redpanda
            # Cela garantit que les données d'une même station sont dans la même partition
            key=str(station['stationcode']).encode('utf-8'),
            value=station
        )

    producer.flush()
    producer.close()

    context.log.info(f"✅ {len(stations)} messages envoyés dans Redpanda (topic: velib.raw.status)")

    return dg.MaterializeResult(
        metadata={
            "count": len(stations),
            "topic": "velib.raw.status",
            "status": "Success"
        }
    )