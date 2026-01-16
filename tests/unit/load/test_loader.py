"""
Pruebas unitarias para Loader (solo método público load).
"""

import pytest
import pytest_check as check
import pyarrow as pa
from unittest.mock import MagicMock
from pathlib import Path


class TestLoader:
    """Pruebas para el orquestador Loader (método load)."""

    @pytest.fixture
    def parquet_source(self, tmp_path: Path) -> str:
        """
        Fixture que simula un directorio S3 con archivos Parquet.
        """
        # Simula directorio de output
        source_dir = tmp_path / "output"
        source_dir.mkdir()
        return str(source_dir)

    @pytest.fixture
    def mock_io_operator(self):
        """
        Fixture con un mock de IO_operator.
        """
        mock = MagicMock()
        mock.get_bucket_objects.return_value = [
            {"Key": "output/dim_date/20260101T100000.parquet"},
            {"Key": "output/fact_sales/20260101T100000.parquet"},
        ]
        mock._parse_s3_path.return_value = ("bucket", "output/")
        mock.get_latest_parquet_path.side_effect = (
            lambda table, base: f"s3://bucket/output/{table}/20260101T100000.parquet"
        )
        mock.read_parquet.return_value = pa.table({"col": [1, 2]})

        def save_csv_side_effect(table, path):
            # path es la ruta de exportación, que incluye el nombre de la tabla
            return path

        mock.save_csv.side_effect = save_csv_side_effect
        return mock

    def test_load_exports_all_tables(self, mock_io_operator, parquet_source):
        """
        Debe exportar todas las tablas encontradas en output_data_dir.
        """
        from load.loader import Loader

        loader = Loader(
            IO_operator=mock_io_operator,
            output_data_dir=parquet_source,
            exports_dir=parquet_source + "/exports",
        )
        results = loader.load()
        check.is_in("dim_date", results)
        check.is_in("fact_sales", results)
        check.is_true(results["dim_date"].endswith("dim_date.csv"))
        check.is_true(results["fact_sales"].endswith("fact_sales.csv"))

    def test_load_raises_if_no_parquet(self, mock_io_operator, parquet_source):
        """
        Debe lanzar ValueError si no hay archivo Parquet para una tabla.
        """
        from load.loader import Loader

        # Simula que solo hay una tabla y no hay parquet para esa tabla
        mock_io_operator.get_bucket_objects.return_value = [
            {"Key": "output/dim_date/20260101T100000.parquet"}
        ]

        def get_latest_parquet_path_side_effect(table, base):
            return None if table == "dim_date" else "some_path"

        mock_io_operator.get_latest_parquet_path.side_effect = (
            get_latest_parquet_path_side_effect
        )
        loader = Loader(
            IO_operator=mock_io_operator,
            output_data_dir=parquet_source,
            exports_dir=parquet_source + "/exports",
        )
        with pytest.raises(ValueError):
            loader.load()
