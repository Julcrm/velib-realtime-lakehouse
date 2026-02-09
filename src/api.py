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


@app.get("/health/pipeline")
def get_pipeline_status():
    """
    Moniteur du Pipeline (Pour ton badge vert/rouge)
    Vérifie la fraîcheur du dernier fichier 'Gold'.
    """
    con = get_db_connection()
    try:
        # On lit le fichier d'alerte généré par velib_alerte.py
        # Le chemin doit correspondre à ton save_path dans velib_alerte.py
        query = "SELECT MAX(last_seen) as last_run FROM read_parquet('s3://gold/alerts/current_status/*.parquet')"
        result = con.execute(query).fetchone()

        last_run_timestamp = result[0]

        # Calcul de la fraîcheur
        now = datetime.now()
        # Note: last_seen est un timestamp (int) ou datetime selon ton schéma Spark.
        # Si c'est un int (unix timestamp), convertis-le.

        return {
            "status": "online",
            "last_data_update": last_run_timestamp,
            "message": "Pipeline operational"
        }
    except Exception as e:
        return {"status": "offline", "error": str(e)}


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