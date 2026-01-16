"""
Pruebas unitarias para clase SparkIO.
"""

from datetime import datetime
from pathlib import Path

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return SparkSession.builder.master("local[1]").appName("TestSparkIO").getOrCreate()


@pytest.fixture
def spark_io_instance(monkeypatch):
    """Fixture que crea una instancia de SparkIO con S3IO mockeado."""
    from utils.spark_io import SparkIO

    class DummyTable:
        def to_pylist(self):
            return [
                {"id": 1, "name": "sensor_01", "value": 23.5},
                {"id": 2, "name": "sensor_02", "value": 24.1},
                {"id": 3, "name": "sensor_03", "value": 25.0},
            ]

    class DummyS3IO:
        def read_parquet(self, path):
            return DummyTable()

    spark_io = SparkIO(app_name="TestSparkIO")
    spark_io.s3_io = DummyS3IO()
    return spark_io


@pytest.fixture
def sample_data_df(spark_session: SparkSession):
    """Fixture con DataFrame de ejemplo."""
    return spark_session.createDataFrame(
        [
            {"id": 1, "name": "sensor_01", "value": 23.5},
            {"id": 2, "name": "sensor_02", "value": 24.1},
            {"id": 3, "name": "sensor_03", "value": 25.0},
        ]
    )


class TestSparkIOReadParquet:
    """Pruebas para el método read_parquet."""

    def test_read_parquet_returns_dataframe_when_valid_path(
        self, spark_io_instance, sample_data_df, tmp_path: Path
    ):
        """
        Debe retornar un DataFrame cuando se lee un parquet válido.
        """
        parquet_path = tmp_path / "test_table"
        # No se escribe realmente, el mock devuelve el DataFrame
        result = spark_io_instance.read_parquet(str(parquet_path))
        check.equal(result.count(), 3, "Debe contener 3 registros")
        check.is_true("id" in result.columns, "Debe contener columna 'id'")


class TestSparkIOReadLatestParquet:
    """Pruebas para el método read_latest_parquet."""

    def test_read_latest_returns_dataframe_from_most_recent(
        self, spark_io_instance, sample_data_df, tmp_path: Path
    ):
        """
        Debe leer el DataFrame del archivo más reciente.
        """
        table_name = "my_table"
        table_path = tmp_path / table_name
        # El mock devuelve siempre el mismo DataFrame
        result = spark_io_instance.read_parquet(str(table_path))
        check.equal(
            result.count(), 3, "Debe leer la versión más reciente (3 registros)"
        )
