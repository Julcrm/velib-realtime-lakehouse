#!/bin/bash
set -e

# Configuration commune
cp dagster.yaml $DAGSTER_HOME/dagster.yaml
cp workspace.yaml $DAGSTER_HOME/workspace.yaml

# Routage basé sur la variable d'environnement
if [ "$SERVICE_TYPE" = "api" ]; then
    echo "Démarrage de FastAPI ..."
    exec uv run uvicorn src.api:app --host 0.0.0.0 --port 8000
else
    echo "Démarrage de Dagster ..."
    exec uv run dagster api grpc -h 0.0.0.0 -p 4000 -f src/definitions.py
fi