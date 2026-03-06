# serving/api.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
from datetime import datetime

app = FastAPI(title="Velib Lakehouse API")

# Autoriser React (localhost:3000) à nous parler
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Velib Lakehouse API is running", "version": "0.1.0"}


# Configuration MinIO pour DuckDB
# On utilise les mêmes variables d'env que ton pipeline Dagster
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000").replace("http://", "")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_db_connection():
    """Crée une connexion DuckDB configurée pour MinIO"""
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    return con

@app.get("/alerts/critical")
def get_critical_alerts():
    """
    Retourne les stations qui vont bientôt être vides.
    Lit directement le résultat de ton asset 'velib_critical_alerts'.
    """
    con = get_db_connection()
    try:
        # Requête SQL directe sur le fichier Parquet généré par Spark
        # On récupère les stations marquées CRITICAL_EMPTY ou WARNING_LOW
        query = """
            SELECT 
                station_name, 
                bikes, 
                trend as net_flow, 
                avg_1h,
                alert_level
            FROM read_parquet('s3://gold/alerts/current_status/*.parquet')
            ORDER BY alert_level ASC, bikes ASC
        """
        df = con.execute(query).fetchdf()

        # Conversion en JSON
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health/pipeline")
def get_pipeline_status():
    """
    Data Observability Endpoint avec Réconciliation.
    Croise le référentiel Bronze (Total attendu) avec le Silver (Total réel).
    """
    con = get_db_connection()
    try:
        # La requête "Flex" : On lit le JSON de référence et on le compare au Parquet filtré.
        # DuckDB est assez intelligent pour parser le JSON à la volée.
        query = """
            WITH reference_data AS (
                -- On compte le nombre total de stations dans le référentiel physique
                -- On utilise unnest pour aplatir le tableau JSON 'stations'
                SELECT count(*) as total_expected
                FROM (
                    SELECT UNNEST(data.stations) 
                    FROM read_json_auto('s3://bronze/velib/reference/station_information.json')
                )
            ),
            silver_data AS (
                -- On compte les stations saines et actives dans la couche Silver
                SELECT 
                    MAX(last_reported) as latest_sync,
                    COUNT(DISTINCT station_code) as active_stations,
                    SUM(bikes_available) as total_bikes,
                    SUM(docks_available) as total_docks
                FROM read_parquet('s3://silver/velib_stats/*/*.parquet')
                WHERE date = current_date
            )
            SELECT 
                s.latest_sync,
                s.active_stations,
                r.total_expected,
                (r.total_expected - s.active_stations) as zombie_stations,
                s.total_bikes,
                s.total_docks
            FROM silver_data s
            CROSS JOIN reference_data r
        """

        result = con.execute(query).fetchone()

        return {
            "status": "healthy",
            "metrics": {
                "latest_sync": result[0],
                "active_stations": result[1],
                "expected_stations": result[2],
                "zombie_stations": result[3],  # Voici ton audit de perte !
                "total_bikes_network": result[4],
                "total_docks_network": result[5]
            }
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}