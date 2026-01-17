"""
Módulo de configuración centralizada de rutas y variables de entorno para el pipeline ETL.

Contexto:
- Rol: Configuración (Utilities)
- Propósito: Centraliza rutas, variables de entorno y parámetros de conexión utilizados en todas las fases del pipeline ETL.
- Dependencias clave: dotenv, os, pathlib

Este módulo permite mantener la configuración desacoplada y reutilizable en todo el proyecto.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Raíz del proyecto (funciona tanto en desarrollo local como en Docker)
# En Docker: /app
# En local: directorio padre de 'config'
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Cargar variables desde .env
load_dotenv()

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
OUTPUT_DATA_DIR = os.getenv("OUTPUT_DATA_DIR")
EXPORTS_DIR = os.getenv("EXPORTS_DIR")

# Configuración de la API
API_CONFIG = {
    "base_url": os.getenv("API_BASE_URL"),
    "email": os.getenv("USER_EMAIL"),
    "key": os.getenv("API_KEY"),
    "dataset_type": os.getenv("DATASET_TYPE"),
    "rows": os.getenv("ROWS"),
}


def get_api_url() -> str:
    """
    Construye la URL completa de la API con los parámetros configurados.

    Returns:
        URL formateada para la petición a la API.
    """
    if not all(API_CONFIG.values()):
        raise ValueError(
            "Faltan configuraciones de la API en las variables de entorno."
        )
    return (
        f"{API_CONFIG['base_url']}?"
        f"email={API_CONFIG['email']}&"
        f"key={API_CONFIG['key']}&"
        f"type={API_CONFIG['dataset_type']}&"
        f"rows={API_CONFIG['rows']}"
    )
