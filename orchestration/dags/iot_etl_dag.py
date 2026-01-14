"""
DAG principal del pipeline ETL de IoT.

Ejecuta las tres fases del ETL en contenedores Docker:
1. Extract: Obtiene datos de la API y los guarda en data/raw
2. Transform: Limpia datos y construye modelo dimensional en data/output
3. Load: Exporta las tablas a CSV
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Configuración del DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Configuración de Docker
IMAGE_NAME = os.environ["ETL_IMAGE_NAME"]
PROJECT_ROOT_HOST = os.environ["ETL_PROJECT_ROOT"]

# Volúmenes compartidos entre tareas
MOUNTS = [
    Mount(source=f"{PROJECT_ROOT_HOST}/data", target="/app/data", type="bind"),
    Mount(
        source=f"{PROJECT_ROOT_HOST}/src",
        target="/app/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{PROJECT_ROOT_HOST}/config",
        target="/app/config",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{PROJECT_ROOT_HOST}/.env",
        target="/app/.env",
        type="bind",
        read_only=True,
    ),
]

# Configuración común para DockerOperator
DOCKER_DEFAULTS = {
    "image": IMAGE_NAME,
    "mounts": MOUNTS,
    "auto_remove": True,
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "bridge",
    "mount_tmp_dir": False,
}

dag = DAG(
    "iot_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL para datos de IoT industrial",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "iot", "pyspark"],
)

extract = DockerOperator(
    task_id="extract",
    command="python -m extract.extractor",
    dag=dag,
    **DOCKER_DEFAULTS,
)

transform = DockerOperator(
    task_id="transform",
    command="python -m transform.transformer",
    dag=dag,
    **DOCKER_DEFAULTS,
)

load = DockerOperator(
    task_id="load",
    command="python -m load.loader",
    dag=dag,
    **DOCKER_DEFAULTS,
)

extract >> transform >> load
