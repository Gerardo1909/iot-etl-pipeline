"""
Pruebas unitarias para builders de dimensiones derivadas.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestDerivedDimensions")
        .getOrCreate()
    )


class TestBuildDimProduct:
    """Pruebas para el builder de dim_product."""

    @pytest.fixture
    def sample_orders_df(self, spark: SparkSession):
        """DataFrame de órdenes con productos repetidos."""
        return spark.createDataFrame(
            [
                {"order_id": 1, "product_code": "PROD-A", "quantity_ordered": 100},
                {"order_id": 2, "product_code": "PROD-B", "quantity_ordered": 200},
                {
                    "order_id": 3,
                    "product_code": "PROD-A",
                    "quantity_ordered": 150,
                },  # Repetido
                {"order_id": 4, "product_code": "PROD-C", "quantity_ordered": 300},
            ]
        )

    def test_should_extract_unique_products_with_sk(
        self, spark: SparkSession, sample_orders_df
    ):
        """
        Verifica que se extraigan productos únicos y se genere SK.
        """
        from transform.builders.dimensions.derived_dimensions import build_dim_product

        result = build_dim_product(sample_orders_df)

        # Debe haber 3 productos únicos
        check.equal(result.count(), 3, "Debe haber 3 productos únicos")

        # Verificar que tiene SK
        check.is_true("product_sk" in result.columns, "Debe tener product_sk")
        check.is_true("product_code" in result.columns, "Debe tener product_code")

        # Verificar unicidad de SKs
        sks = [row["product_sk"] for row in result.collect()]
        check.equal(len(sks), len(set(sks)), "SKs deben ser únicos")


class TestBuildDimAlertType:
    """Pruebas para el builder de dim_alert_type."""

    @pytest.fixture
    def sample_alerts_df(self, spark: SparkSession):
        """DataFrame de alertas."""
        return spark.createDataFrame(
            [
                {"alert_id": 1, "alert_type": "threshold_exceeded", "severity": "high"},
                {"alert_id": 2, "alert_type": "threshold_exceeded", "severity": "low"},
                {
                    "alert_id": 3,
                    "alert_type": "machine_offline",
                    "severity": "critical",
                },
                {
                    "alert_id": 4,
                    "alert_type": "threshold_exceeded",
                    "severity": "high",
                },  # Repetido
            ]
        )

    def test_should_categorize_alerts_by_type_and_severity(
        self, spark: SparkSession, sample_alerts_df
    ):
        """
        Verifica que se extraigan combinaciones únicas de tipo+severidad.
        """
        from transform.builders.dimensions.derived_dimensions import (
            build_dim_alert_type,
        )

        result = build_dim_alert_type(sample_alerts_df)

        # 3 combinaciones únicas: (threshold_exceeded, high), (threshold_exceeded, low), (machine_offline, critical)
        check.equal(
            result.count(), 3, "Debe haber 3 combinaciones únicas tipo+severidad"
        )

        # Verificar columnas
        check.is_true("alert_type_sk" in result.columns, "Debe tener alert_type_sk")
        check.is_true("category" in result.columns, "Debe tener category")
        check.is_true("requires_ack" in result.columns, "Debe tener requires_ack")


class TestBuildDimMaintenanceType:
    """Pruebas para el builder de dim_maintenance_type."""

    @pytest.fixture
    def sample_maintenance_df(self, spark: SparkSession):
        """DataFrame de logs de mantenimiento."""
        return spark.createDataFrame(
            [
                {"log_id": 1, "maintenance_type": "preventive"},
                {"log_id": 2, "maintenance_type": "corrective"},
                {"log_id": 3, "maintenance_type": "preventive"},  # Repetido
                {"log_id": 4, "maintenance_type": "predictive"},
            ]
        )

    def test_should_extract_unique_maintenance_types(
        self, spark: SparkSession, sample_maintenance_df
    ):
        """
        Verifica que se extraigan tipos de mantenimiento únicos.
        """
        from transform.builders.dimensions.derived_dimensions import (
            build_dim_maintenance_type,
        )

        result = build_dim_maintenance_type(sample_maintenance_df)

        # 3 tipos únicos
        check.equal(result.count(), 3, "Debe haber 3 tipos de mantenimiento únicos")

        # Verificar columnas
        check.is_true("maint_type_sk" in result.columns, "Debe tener maint_type_sk")
        check.is_true("maint_type_name" in result.columns, "Debe tener maint_type_name")

        # Verificar valores
        types = [row["maint_type_name"] for row in result.collect()]
        check.is_true("preventive" in types, "Debe incluir preventive")
        check.is_true("corrective" in types, "Debe incluir corrective")
        check.is_true("predictive" in types, "Debe incluir predictive")
