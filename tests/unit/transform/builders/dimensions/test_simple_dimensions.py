"""
Pruebas unitarias para build_simple_dimension.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestSimpleDimensions")
        .getOrCreate()
    )


class TestBuildSimpleDimension:
    """Pruebas para el builder de dimensiones simples."""

    @pytest.fixture
    def sample_factories_df(self, spark: SparkSession):
        """DataFrame de fábricas de ejemplo."""
        return spark.createDataFrame(
            [
                {
                    "factory_id": 1,
                    "factory_name": "Factory Alpha",
                    "location": "New York",
                    "region": "East",
                    "manager_name": "John Doe",
                    "total_area_sqm": 5000,
                },
                {
                    "factory_id": 2,
                    "factory_name": "Factory Beta",
                    "location": "Los Angeles",
                    "region": "West",
                    "manager_name": "Jane Smith",
                    "total_area_sqm": 7500,
                },
            ]
        )

    def test_should_create_surrogate_key_from_natural_key(
        self, spark: SparkSession, sample_factories_df
    ):
        """
        Verifica que se cree la SK a partir de la NK.
        """
        from transform.builders.dimensions.simple_dimensions import (
            build_simple_dimension,
            SimpleDimensionConfig,
        )

        config = SimpleDimensionConfig(
            source_table="factories",
            natural_key="factory_id",
            surrogate_key="factory_sk",
            columns=["factory_id", "factory_name"],
        )

        result = build_simple_dimension(sample_factories_df, config)

        check.is_true("factory_sk" in result.columns, "Debe tener columna factory_sk")

        # Verificar que SK tiene valores correctos (iguales a NK)
        rows = result.collect()
        sks = [row["factory_sk"] for row in rows]
        check.equal(sorted(sks), [1, 2], "SKs deben ser 1 y 2")

    def test_should_select_only_configured_columns(
        self, spark: SparkSession, sample_factories_df
    ):
        """
        Verifica que solo se incluyan las columnas configuradas más la SK.
        """
        from transform.builders.dimensions.simple_dimensions import (
            build_simple_dimension,
            SimpleDimensionConfig,
        )

        config = SimpleDimensionConfig(
            source_table="factories",
            natural_key="factory_id",
            surrogate_key="factory_sk",
            columns=["factory_id", "factory_name", "region"],
        )

        result = build_simple_dimension(sample_factories_df, config)

        # Debe tener: factory_sk, factory_id (original), factory_name, region
        expected_columns = {"factory_sk", "factory_id", "factory_name", "region"}
        actual_columns = set(result.columns)

        check.equal(
            actual_columns,
            expected_columns,
            "Debe incluir columnas configuradas más la SK agregada",
        )

    def test_should_place_surrogate_key_as_first_column(
        self, spark: SparkSession, sample_factories_df
    ):
        """
        Verifica que la SK sea la primera columna del DataFrame.
        """
        from transform.builders.dimensions.simple_dimensions import (
            build_simple_dimension,
            SimpleDimensionConfig,
        )

        config = SimpleDimensionConfig(
            source_table="factories",
            natural_key="factory_id",
            surrogate_key="factory_sk",
            columns=["factory_id", "factory_name"],
        )

        result = build_simple_dimension(sample_factories_df, config)

        check.equal(result.columns[0], "factory_sk", "SK debe ser la primera columna")
