"""
Pruebas unitarias para clase SparkIO.
"""

from datetime import datetime
from pathlib import Path

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return SparkSession.builder.master("local[1]").appName("TestSparkIO").getOrCreate()


@pytest.fixture
def spark_io(spark: SparkSession):
    """Fixture que crea una instancia de SparkIO con sesión existente."""
    from src.utils.spark_io import SparkIO

    sio = SparkIO.__new__(SparkIO)
    sio.spark = spark
    return sio


@pytest.fixture
def sample_data(spark: SparkSession):
    """Fixture con DataFrame de ejemplo."""
    return spark.createDataFrame(
        [
            {"id": 1, "name": "sensor_01", "value": 23.5},
            {"id": 2, "name": "sensor_02", "value": 24.1},
            {"id": 3, "name": "sensor_03", "value": 25.0},
        ]
    )


class TestSparkIOReadParquet:
    """Pruebas para el método read_parquet."""

    def test_read_parquet_should_return_dataframe_when_valid_path(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que se retorne un DataFrame cuando se lee un parquet válido.
        """
        # Guardar datos de prueba
        parquet_path = tmp_path / "test_table"
        sample_data.write.parquet(str(parquet_path))

        result = spark_io.read_parquet(parquet_path)

        check.equal(result.count(), 3, "Debe contener 3 registros")
        check.is_true("id" in result.columns, "Debe contener columna 'id'")


class TestSparkIOWriteParquet:
    """Pruebas para el método write_parquet."""

    def test_write_parquet_should_create_directory_when_not_exists(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que se cree el directorio si no existe al escribir.
        """
        output_path = tmp_path / "new_dir" / "output"

        result = spark_io.write_parquet(sample_data, output_path)

        check.is_true(result.exists(), "El directorio debe existir")

    def test_write_parquet_should_persist_data_correctly(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que los datos se persistan correctamente.
        """
        output_path = tmp_path / "output_table"
        spark_io.write_parquet(sample_data, output_path)

        df_read = spark_io.read_parquet(output_path)

        check.equal(df_read.count(), 3, "Debe contener 3 registros")


class TestSparkIOWriteTimestampedParquet:
    """Pruebas para el método write_timestamped_parquet."""

    def test_write_timestamped_should_create_file_with_timestamp(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que se cree un archivo con timestamp en el nombre.
        """
        table_path = tmp_path / "timestamped_table"
        timestamp = datetime(2026, 1, 5, 12, 30, 45)

        result = spark_io.write_timestamped_parquet(sample_data, table_path, timestamp)

        check.is_true(result.exists(), "El archivo debe existir")
        check.is_true(
            "20260105T123045" in result.name, "El nombre debe contener el timestamp"
        )
        check.equal(result.suffix, ".parquet", "La extensión debe ser .parquet")

    def test_write_timestamped_should_use_current_time_when_no_timestamp(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que use la fecha actual cuando no se proporciona timestamp.
        """
        table_path = tmp_path / "auto_timestamp"
        today = datetime.now().strftime("%Y%m%d")

        result = spark_io.write_timestamped_parquet(sample_data, table_path)

        check.is_true(
            today in result.name, f"El nombre debe contener la fecha actual {today}"
        )


class TestSparkIOGetLatestParquet:
    """Pruebas para el método get_latest_parquet."""

    def test_get_latest_should_return_most_recent_file(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que retorne el archivo más reciente según timestamp.
        """
        table_path = tmp_path / "versioned_table"
        table_path.mkdir(parents=True)

        # Crear archivos con diferentes timestamps
        (table_path / "20260101T100000.parquet").touch()
        (table_path / "20260103T150000.parquet").touch()
        (table_path / "20260102T120000.parquet").touch()

        result = spark_io.get_latest_parquet(table_path)

        check.equal(
            result.name,
            "20260103T150000.parquet",
            "Debe retornar el archivo más reciente",
        )

    def test_get_latest_should_return_none_when_empty_directory(
        self, spark_io, tmp_path: Path
    ):
        """
        Verifica que retorne None cuando el directorio está vacío.
        """
        empty_path = tmp_path / "empty_table"
        empty_path.mkdir()

        result = spark_io.get_latest_parquet(empty_path)

        check.is_none(result, "Debe retornar None para directorio vacío")


class TestSparkIOReadLatestParquet:
    """Pruebas para el método read_latest_parquet."""

    def test_read_latest_should_return_dataframe_from_most_recent(
        self, spark_io, sample_data, tmp_path: Path
    ):
        """
        Verifica que lea el DataFrame del archivo más reciente.
        """
        table_name = "my_table"
        table_path = tmp_path / table_name

        # Escribir versión antigua
        spark_io.write_timestamped_parquet(
            sample_data.limit(1), table_path, datetime(2026, 1, 1)
        )

        # Escribir versión más reciente con más datos
        spark_io.write_timestamped_parquet(
            sample_data, table_path, datetime(2026, 1, 5)
        )

        result = spark_io.read_latest_parquet(table_name, tmp_path)

        check.equal(
            result.count(), 3, "Debe leer la versión más reciente (3 registros)"
        )

    def test_read_latest_should_return_none_when_table_not_exists(
        self, spark_io, tmp_path: Path
    ):
        """
        Verifica que retorne None cuando la tabla no existe.
        """
        result = spark_io.read_latest_parquet("nonexistent_table", tmp_path)

        check.is_none(result, "Debe retornar None para tabla inexistente")
