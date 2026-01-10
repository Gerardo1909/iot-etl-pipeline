"""
Pruebas unitarias para build_dim_date.
"""

import pytest
import pytest_check as check
from datetime import date
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return SparkSession.builder.master("local[1]").appName("TestDimDate").getOrCreate()


class TestBuildDimDate:
    """Pruebas para el builder de dim_date."""

    def test_should_generate_expected_row_count_for_date_range(
        self, spark: SparkSession
    ):
        """
        Verifica que se genere el número correcto de filas para el rango de fechas.
        """
        from transform.builders.dimensions.dim_date import build_dim_date

        # Rango pequeño para test: 10 días
        start = date(2024, 1, 1)
        end = date(2024, 1, 10)

        result = build_dim_date(spark, start_date=start, end_date=end)

        check.equal(result.count(), 10, "Debe generar 10 filas para 10 días")

    def test_should_have_correct_schema_and_key_attributes(self, spark: SparkSession):
        """
        Verifica que el DataFrame tenga el esquema correcto con atributos clave.
        """
        from transform.builders.dimensions.dim_date import build_dim_date

        result = build_dim_date(
            spark, start_date=date(2024, 1, 1), end_date=date(2024, 1, 1)
        )

        # Verificar columnas esperadas
        expected_columns = {
            "date_sk",
            "date_actual",
            "day_of_week",
            "day_name",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
            "is_holiday",
            "fiscal_period",
        }
        actual_columns = set(result.columns)

        check.equal(
            actual_columns,
            expected_columns,
            "Debe tener todas las columnas del esquema",
        )

        # Verificar valores para fecha conocida (1 de enero 2024 = lunes)
        row = result.collect()[0]
        check.equal(row["date_sk"], 20240101, "date_sk debe ser formato YYYYMMDD")
        check.equal(row["year"], 2024, "year debe ser 2024")
        check.equal(row["month_number"], 1, "month_number debe ser 1")
        check.equal(row["quarter"], 1, "quarter debe ser 1")
