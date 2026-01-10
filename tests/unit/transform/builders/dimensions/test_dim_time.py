"""
Pruebas unitarias para build_dim_time.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return SparkSession.builder.master("local[1]").appName("TestDimTime").getOrCreate()


class TestBuildDimTime:
    """Pruebas para el builder de dim_time."""

    def test_should_generate_24_rows_for_hours(self, spark: SparkSession):
        """
        Verifica que se generen exactamente 24 filas (una por hora).
        """
        from transform.builders.dimensions.dim_time import build_dim_time

        result = build_dim_time(spark)

        check.equal(result.count(), 24, "Debe generar 24 filas para las 24 horas")

    def test_should_classify_time_of_day_correctly(self, spark: SparkSession):
        """
        Verifica que time_of_day clasifique correctamente las horas.
        """
        from transform.builders.dimensions.dim_time import build_dim_time

        result = build_dim_time(spark)
        rows = {row["hour_of_day"]: row for row in result.collect()}

        # Night: 0-5
        check.equal(rows[3]["time_of_day"], "Night", "Hora 3 debe ser Night")

        # Morning: 6-11
        check.equal(rows[9]["time_of_day"], "Morning", "Hora 9 debe ser Morning")

        # Afternoon: 12-17
        check.equal(rows[14]["time_of_day"], "Afternoon", "Hora 14 debe ser Afternoon")

        # Evening: 18-23
        check.equal(rows[20]["time_of_day"], "Evening", "Hora 20 debe ser Evening")

        # Verificar AM/PM
        check.equal(rows[10]["period_am_pm"], "AM", "Hora 10 debe ser AM")
        check.equal(rows[15]["period_am_pm"], "PM", "Hora 15 debe ser PM")
