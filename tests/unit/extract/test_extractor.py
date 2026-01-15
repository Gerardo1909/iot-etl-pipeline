"""
Pruebas unitarias para clase Extractor.
"""

import pytest
import pytest_check as check
from unittest.mock import Mock, patch


class TestExtractorGetTableFromPayload:
    """Pruebas para el método _get_table_from_payload."""

    @pytest.fixture
    def sample_payload(self) -> dict:
        """Fixture con payload de ejemplo."""
        return {
            "tables": {
                "sensores": [
                    {"id": 1, "nombre": "temp_01"},
                    {"id": 2, "nombre": "hum_01"},
                ],
                "dispositivos": [
                    {"id": 1, "ubicacion": "sala"},
                ],
            }
        }

    def test_get_table_returns_data_when_table_exists(self, sample_payload):
        """
        Debe retornar los datos correctos cuando la tabla existe en el payload.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_io_operator = Mock()
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        result = extractor._get_table_from_payload(sample_payload, "sensores")
        check.is_not_none(result, "Debe retornar datos cuando la tabla existe")
        check.equal(len(result), 2, "Debe contener 2 registros")

    def test_get_table_returns_none_when_table_not_exists(self, sample_payload):
        """
        Debe retornar None cuando la tabla no existe en el payload.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_io_operator = Mock()
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        result = extractor._get_table_from_payload(sample_payload, "tabla_inexistente")
        check.is_none(result, "Debe retornar None cuando la tabla no existe")

    def test_get_table_returns_none_when_payload_has_no_tables(self):
        """
        Debe retornar None cuando el payload no contiene la clave 'tables'.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_io_operator = Mock()
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        result = extractor._get_table_from_payload({}, "sensores")
        check.is_none(result, "Debe retornar None cuando no hay tablas")


class TestExtractorExtract:
    """Pruebas para el método extract."""

    def test_extract_returns_dict_with_saved_paths_when_successful(self):
        """
        Debe retornar un diccionario con las rutas guardadas cuando la extracción es exitosa.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
            }
        }
        mock_io_operator = Mock()
        mock_io_operator.save_parquet.return_value = (
            "s3://bucket/raw/sensores/20260101T100000.parquet"
        )
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        result = extractor.extract()
        check.is_instance(result, dict, "Debe retornar un diccionario")
        check.is_true("sensores" in result, "Debe contener la tabla 'sensores'")

    def test_extract_calls_save_parquet_for_each_table(self):
        """
        Debe llamar a save_parquet del IO_operator para cada tabla durante la extracción.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
                "lecturas": [{"valor": 25}],
            }
        }
        mock_io_operator = Mock()
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        extractor.extract()
        check.equal(
            mock_io_operator.save_parquet.call_count,
            2,
            "Debe llamar save_parquet una vez por tabla",
        )

    def test_extract_skips_missing_tables_not_in_payload(self):
        """
        Debe saltar tablas faltantes durante la extracción si no se encuentran en el payload.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
            }
        }
        mock_io_operator = Mock()
        extractor = Extractor(
            http_client=mock_client,
            IO_operator=mock_io_operator,
            raw_data_dir="s3://bucket/raw",
        )
        result = extractor.extract()
        check.equal(len(result), 1, "Solo debe guardar tablas que existen")
        check.is_true(
            "tabla_no_existe" not in result, "No debe incluir tablas faltantes"
        )
