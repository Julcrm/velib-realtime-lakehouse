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


@app.get("/alerts/critical")
def get_critical_alerts():
    con = get_db_connection()
    try:
        query = """
            WITH target_stations AS (
                SELECT station_code, station_name, bikes
                FROM read_parquet('s3://gold/alerts/current_status/*.parquet')
                WHERE bikes <= 5 -- On élargit un peu pour avoir de la donnée
            ),
            historical_trends AS (
                SELECT 
                    station_code,
                    bikes_available,
                    last_reported,
                    ROW_NUMBER() OVER (PARTITION BY station_code ORDER BY last_reported DESC) as rank
                FROM read_parquet('s3://silver/velib_stats/*/*.parquet')
                WHERE station_code IN (SELECT station_code FROM target_stations)
            )
            SELECT 
                t.station_name,
                t.bikes as current_bikes,
                LIST(h.bikes_available ORDER BY h.last_reported ASC) as sparkline_data
            FROM target_stations t
            JOIN historical_trends h ON t.station_code = h.station_code
            WHERE h.rank <= 5
            GROUP BY t.station_name, t.bikes
            ORDER BY current_bikes ASC
        """

        cursor = con.execute(query)
        columns = [column_meta[0] for column_meta in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        # --- SYNCHRONISATION ICI ---
        # On calcule les agrégats à la volée avant de renvoyer
        critical_count = sum(1 for s in results if s['current_bikes'] == 0)
        warning_count = sum(1 for s in results if 0 < s['current_bikes'] <= 3)

        return {
            "stations": results,
            "summary": {
                "critical_empty": critical_count,
                "warning_low": warning_count
            }
        }
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