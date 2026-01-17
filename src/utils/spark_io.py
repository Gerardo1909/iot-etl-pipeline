"""
Módulo de utilidades para operaciones Spark y acceso a datos en el pipeline ETL.

Contexto:
- Rol: Utilidades (Utilities)
- Propósito: Centraliza la configuración de PySpark y las funciones de entrada/salida (I/O) con S3, facilitando la interoperabilidad entre Spark y pyarrow.
- Dependencias clave: PySpark, pyarrow, S3IO

Este módulo permite la integración eficiente entre Spark y almacenamiento en la nube para el pipeline ETL.
"""

from pyspark.sql import DataFrame, SparkSession

import pyarrow as pa


from utils.s3_io import S3IO


class SparkIO:
    """
    Abstracción de operaciones Spark y acceso a datos en el pipeline ETL.

    Responsabilidad:
    - Inicializar y exponer la sesión de Spark.
    - Leer y escribir datos en S3 usando pyarrow y Spark DataFrame.
    - Facilitar la interoperabilidad entre Spark y almacenamiento en la nube.

    Uso:
    Instanciar y utilizar para todas las operaciones de I/O y sesión Spark en el pipeline.
    """

    def __init__(self, app_name: str = "IOT_ETL"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.s3_io = S3IO()

    @property
    def session(self) -> SparkSession:
        """Expone la sesión para operaciones avanzadas."""
        return self.spark

    def read_parquet(self, path: str) -> DataFrame:
        """
        Lee un archivo Parquet desde S3 usando S3IO (pyarrow+boto3) y lo convierte a Spark DataFrame.
        """

        table = self.s3_io.read_parquet(path)
        data = table.to_pylist()
        return self.spark.createDataFrame(data)

    def write_parquet(self, df: DataFrame, path: str) -> str:
        """
        Escribe un DataFrame en formato Parquet en S3 usando S3IO (pyarrow+boto3).
        """
        # Convertir Spark DataFrame a lista de dicts y luego a pyarrow Table
        data = [row.asDict() for row in df.collect()]
        table = pa.Table.from_pylist(data)
        self.s3_io.save_parquet(table.to_pylist(), path)
        return path

    def read_latest_parquet(self, table_name: str, base_path: str) -> pa.Table:
        """
        Lee el archivo Parquet más reciente basado en la marca de tiempo en el nombre del archivo en S3.
        """
        s3_path = self.s3_io.get_latest_parquet_path(table_name, base_path)
        return self.read_parquet(s3_path)
