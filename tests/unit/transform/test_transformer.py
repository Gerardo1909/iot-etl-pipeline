"""
Pruebas unitarias para clase Transformer.
"""

import pytest
import pytest_check as check
from pathlib import Path
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]").appName("TestTransformer").getOrCreate()
    )


@pytest.fixture
def mock_spark_io(spark: SparkSession):
    """Fixture que crea un mock de SparkIO."""
    mock = Mock()
    mock.spark = spark
    return mock


@pytest.fixture
def sample_alerts_df(spark: SparkSession):
    """DataFrame de alertas de ejemplo."""
    return spark.createDataFrame(
        [
            {
                "alert_id": "A001",
                "acknowledged_at": None,
                "resolved_at": None,
                "resolved_by": None,
            },
            {
                "alert_id": "A002",
                "acknowledged_at": None,
                "resolved_at": None,
                "resolved_by": None,
            },
            {
                "alert_id": "A003",
                "acknowledged_at": "2025-04-23 09:37:56",
                "resolved_at": "2025-05-23 09:37:56",
                "resolved_by": "worker_123",
            },
        ]
    )


@pytest.fixture
def sample_factories_df(spark: SparkSession):
    """DataFrame de fábricas de ejemplo."""
    return spark.createDataFrame(
        [
            {"factory_id": "F001", "name": "Factory 1"},
            {"factory_id": "F002", "name": "Factory 2"},
        ]
    )


class TestTransformerInit:
    """Pruebas para la inicialización del Transformer."""

    def test_init_sets_spark_io(self, mock_spark_io):
        """
        Debe configurar correctamente el atributo spark_io.
        """
        from transform.transformer import Transformer

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )

        check.equal(transformer.spark_io, mock_spark_io)

    def test_init_has_cleaners_registry(self, mock_spark_io):
        """
        Debe existir el registro de cleaners.
        """
        from transform.transformer import Transformer

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )

        check.is_true(hasattr(transformer, "CLEANERS"))
        check.is_true(len(transformer.CLEANERS) > 0)

    def test_init_has_tables_registry(self, mock_spark_io):
        """
        Debe existir el registro de tablas.
        """
        from transform.transformer import Transformer

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )

        check.is_true(hasattr(transformer, "TABLES"))
        check.is_true(len(transformer.TABLES) > 0)


class TestTransformerCleanTables:
    """Pruebas para el método _clean_tables."""

    def test_clean_tables_returns_dict(self, mock_spark_io, sample_alerts_df):
        """
        Debe retornar un diccionario con los resultados de limpieza.
        """
        from transform.transformer import Transformer

        # Configurar mock para retornar solo una tabla
        mock_spark_io.read_latest_parquet = Mock(
            side_effect=lambda name, path: sample_alerts_df
            if name == "alerts"
            else None
        )
        mock_spark_io.write_timestamped_parquet = Mock()

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )
        result = transformer._clean_tables()

        check.is_instance(result, dict)

    def test_clean_tables_skips_nonexistent_tables(self, mock_spark_io):
        """
        Debe omitir las tablas que no existen.
        """
        from transform.transformer import Transformer

        mock_spark_io.read_latest_parquet = Mock(return_value=None)
        mock_spark_io.write_timestamped_parquet = Mock()

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )
        result = transformer._clean_tables()

        check.equal(len(result), 0, "No debe haber tablas procesadas")

    def test_clean_tables_uses_specialized_cleaner_for_alerts(
        self, mock_spark_io, sample_alerts_df
    ):
        """
        Debe usar AlertsCleaner para la tabla alerts.
        """
        from transform.transformer import Transformer

        # Solo retornar datos para alerts
        def mock_read(name, path):
            if name == "alerts":
                return sample_alerts_df
            return None

        mock_spark_io.read_latest_parquet = Mock(side_effect=mock_read)
        mock_spark_io.write_timestamped_parquet = Mock()

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )
        result = transformer._clean_tables()

        # Verificar que se procesó alerts
        check.is_true("alerts" in result, "alerts debe estar en el resultado")

        # Verificar que se añadió la columna alert_status (específica de AlertsCleaner)
        check.is_true(
            "alert_status" in result["alerts"].columns,
            "Debe tener columna alert_status del cleaner especializado",
        )

    def test_clean_tables_calls_write_for_each_table(
        self, mock_spark_io, sample_alerts_df, sample_factories_df
    ):
        """
        Debe llamar a write para cada tabla procesada.
        """
        from transform.transformer import Transformer

        def mock_read(name, path):
            if name == "alerts":
                return sample_alerts_df
            if name == "factories":
                return sample_factories_df
            return None

        mock_spark_io.read_latest_parquet = Mock(side_effect=mock_read)
        call_counter = {"count": 0}

        def fake_write(*args, **kwargs):
            call_counter["count"] += 1

        mock_spark_io.write_parquet = fake_write

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir="raw",
            processed_data_dir="processed",
            output_data_dir="output",
        )
        result = transformer._clean_tables()
        # Debe llamar write dos veces (alerts y factories)
        check.equal(
            call_counter["count"], 2, "Debe llamar write 2 veces (alerts y factories)"
        )


class TestTransformerTransform:
    """Pruebas para el método transform."""

    def test_transform_calls_clean_tables(self, mock_spark_io):
        """
        Debe llamar a _clean_tables cuando se ejecuta transform.
        """
        from transform.transformer import Transformer

        mock_spark_io.read_latest_parquet = Mock(return_value=None)

        transformer = Transformer(
            spark_io=mock_spark_io,
            raw_data_dir=Path("raw"),
            processed_data_dir=Path("processed"),
            output_data_dir=Path("output"),
        )

        with patch.object(transformer, "_clean_tables", return_value={}) as mock_clean:
            transformer.transform()
            mock_clean.assert_called_once()
