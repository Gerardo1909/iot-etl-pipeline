"""
Módulo orquestador del proceso de transformación.
"""

from pathlib import Path
from typing import Dict, Type

from pyspark.sql import DataFrame

from utils.spark_io import SparkIO
from transform.cleaners.base_cleaner import BaseCleaner
from transform.cleaners.alerts_cleaner import AlertsCleaner
from transform.cleaners.quality_checks_cleaner import QualityChecksCleaner
from transform.cleaners.defects_cleaner import DefectsCleaner
from transform.cleaners.maintenance_logs_cleaner import MaintenanceLogsCleaner


# Configuración de guardado
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DATA_DIR = DATA_DIR / "output"


class Transformer:
    """
    Orquesta el proceso de transformación del pipeline ETL.

    Flujo:
        Bronze (raw) → Cleaners → Silver (processed) → Builders → Gold (output)
    """

    # Registro de cleaners especializados por tabla
    CLEANERS: Dict[str, Type[BaseCleaner]] = {
        "alerts": AlertsCleaner,
        "quality_checks": QualityChecksCleaner,
        "defects": DefectsCleaner,
        "maintenance_logs": MaintenanceLogsCleaner,
    }

    # Diccionario de tablas a procesar con su id de columna principal
    TABLES = {
        "alerts": "alert_id",
        "defects": "defect_id",
        "factories": "factory_id",
        "machines": "machine_id",
        "maintenance_logs": "log_id",
        "maintenance_schedules": "schedule_id",
        "operators": "operator_id",
        "production_lines": "line_id",
        "production_orders": "order_id",
        "production_output": "output_id",
        "quality_checks": "check_id",
        "sensor_readings": "reading_id",
        "sensors": "sensor_id",
        "shifts": "shift_id",
    }

    def __init__(self, spark_io: SparkIO):
        """
        Args:
            spark_io: Instancia de SparkIO para operaciones de lectura/escritura
        """
        self.spark_io = spark_io

    def transform(self) -> None:
        """
        Ejecuta el pipeline completo de transformación.
        """
        # Fase 1: Limpiar tablas (Bronze → Silver)
        cleaned_tables = self._clean_tables()

        # Fase 2: Construir dimensiones (Silver → Gold)
        # dimensions = self._build_dimensions(cleaned_tables)

        # Fase 3: Construir hechos (Silver + Dims → Gold)
        # facts = self._build_facts(cleaned_tables, dimensions)

    def _clean_tables(self) -> Dict[str, DataFrame]:
        """
        Limpia todas las tablas raw y las guarda en processed (Silver).

        Returns:
            Diccionario con los DataFrames limpios
        """
        cleaned = {}

        for table_name, id_column in self.TABLES.items():
            # Leer la versión más reciente de la tabla raw
            df = self.spark_io.read_latest_parquet(table_name, RAW_DATA_DIR)
            if df is None:
                continue

            # Obtener cleaner apropiado (específico o base)
            cleaner = self.CLEANERS.get(table_name, BaseCleaner)
            cleaner = cleaner(df, id_column)

            # Ejecutar limpieza
            df_cleaned = cleaner.clean()

            # Guardar en processed con timestamp (permite versionado)
            self.spark_io.write_timestamped_parquet(
                df_cleaned, PROCESSED_DATA_DIR / table_name
            )
            cleaned[table_name] = df_cleaned

        return cleaned

    def _build_dimensions(self, cleaned: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Construye todas las tablas dimensionales y las guarda en el
        directorio 'output'.

        Args:
            cleaned: Diccionario de DataFrames limpios

        Returns:
            Diccionario con los DataFrames de dimensiones
        """
        dimensions = {}

        # TODO: Implementar builders de dimensiones

        # Guardar cada dimensión en Gold
        for dim_name, df in dimensions.items():
            self.spark_io.write_parquet(df, OUTPUT_DATA_DIR / dim_name)

        return dimensions

    def _build_facts(
        self, cleaned: Dict[str, DataFrame], dimensions: Dict[str, DataFrame]
    ) -> Dict[str, DataFrame]:
        """
        Construye todas las tablas de hechos y las guarda en el
        directorio 'output'

        Args:
            cleaned: Diccionario de DataFrames limpios
            dimensions: Diccionario de DataFrames de dimensiones

        Returns:
            Diccionario con los DataFrames de hechos
        """
        facts = {}

        # TODO: Implementar builders de hechos

        # Guardar cada fact en 'output'
        for fact_name, df in facts.items():
            self.spark_io.write_parquet(df, OUTPUT_DATA_DIR / fact_name)

        return facts
