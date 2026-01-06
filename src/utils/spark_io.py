"""
Módulo que centraliza la configuración de PySpark y las
funciones de I/O relacionadas.
"""

import shutil
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from typing import Optional


class SparkIO:
    """
    Centraliza la configuración de PySpark y operaciones de I/O
    para la capa de transformación del pipeline ETL.
    """

    def __init__(self, app_name: str = "IOT_ETL"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    @property
    def session(self) -> SparkSession:
        """Expone la sesión para operaciones avanzadas."""
        return self.spark

    def read_parquet(self, path: Path) -> DataFrame:
        """Lee todos los archivos parquet de un directorio."""
        return self.spark.read.parquet(str(path))

    def write_parquet(self, df: DataFrame, path: Path, mode: str = "overwrite") -> Path:
        """Escribe DataFrame como parquet (formato Spark estándar con particiones)."""
        path.mkdir(parents=True, exist_ok=True)
        df.write.mode(mode).parquet(str(path))
        return path

    def write_timestamped_parquet(
        self, df: DataFrame, table_path: Path, timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Escribe DataFrame como un único archivo parquet con timestamp en el nombre.

        Esto permite identificar versiones y usar read_latest_parquet para
        obtener la versión más reciente.

        Args:
            df: DataFrame a guardar
            table_path: Directorio de la tabla (ej: processed/alerts)
            timestamp: Timestamp para el nombre. Si es None, usa datetime.now()

        Returns:
            Path al archivo parquet creado
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Crear directorio si no existe
        table_path.mkdir(parents=True, exist_ok=True)

        # Nombre del archivo con timestamp (formato ISO sin caracteres especiales)
        filename = f"{timestamp.strftime('%Y%m%dT%H%M%S')}.parquet"
        final_path = table_path / filename

        # Directorio temporal para escritura de Spark
        temp_dir = table_path / "_temp_spark_output"

        # Escribir como un solo archivo usando coalesce(1)
        df.coalesce(1).write.mode("overwrite").parquet(str(temp_dir))

        # Encontrar el archivo part-*.parquet generado
        part_files = list(temp_dir.glob("part-*.parquet"))
        if not part_files:
            # Buscar también con extensión snappy
            part_files = list(temp_dir.glob("part-*.snappy.parquet"))

        if part_files:
            # Mover y renombrar el archivo
            shutil.move(str(part_files[0]), str(final_path))

        # Limpiar directorio temporal
        shutil.rmtree(temp_dir, ignore_errors=True)

        return final_path

    def read_latest_parquet(self, table_name: str, file_path: Path) -> DataFrame | None:
        """
        Lee el archivo parquet más reciente de una tabla en
        el directorio especificado.

        Utiliza el timestamp en el nombre del archivo para determinar
        cuál es el más reciente.
        """
        table_path = file_path / table_name
        if not table_path.exists():
            return None

        latest_file = self.get_latest_parquet(table_path)
        if latest_file is None:
            return None

        return self.read_parquet(latest_file)

    def get_latest_parquet(self, table_path: Path) -> Path | None:
        """
        Obtiene el archivo parquet más reciente de un directorio.
        """
        parquet_files = list(table_path.glob("*.parquet"))
        if not parquet_files:
            return None
        return sorted(parquet_files, key=lambda f: f.stem)[-1]
