"""
Módulo de configuración de DAG para orquestación del flujo ETL.

Contexto:
- Rol: Ejecución de orquestación (Airflow DAG)
- Propósito: Se encarga de configurar y orquestar las tareas del pipeline ETL usando contenedores Docker.
- Dependencias clave: airflow, docker, os, datetime

Este módulo permite configurar y ejecutar el flujo ETL de manera automatizada y reproducible.
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
IMAGE_NAME = os.getenv("ETL_IMAGE_NAME")
if not IMAGE_NAME:
    raise RuntimeError(
        "La variable de entorno 'ETL_IMAGE_NAME' debe estar configurada para el DAG ETL."
    )
PROJECT_ROOT_HOST = os.getenv("ETL_PROJECT_ROOT")
if not PROJECT_ROOT_HOST:
    raise RuntimeError(
        "La variable de entorno 'ETL_PROJECT_ROOT' debe estar configurada para el DAG ETL."
    )

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
