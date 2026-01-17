"""
Módulo builder para la tabla de hechos 'fact_quality' en el pipeline ETL.

Contexto:
- Fase: Transformación (Transform)
- Propósito: Construye la tabla de hechos de calidad, integrando inspecciones y defectos para calcular métricas de calidad.
- Dependencias clave: PySpark

Este módulo implementa la lógica de integración y cálculo de métricas de calidad para el modelo dimensional.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_quality(
    quality_checks_df: DataFrame,
    dim_date: DataFrame,
    dim_production_line: DataFrame,
    dim_product: DataFrame,
    dim_order: DataFrame,
    dim_operator: DataFrame
) -> DataFrame:
    """
    Construye la tabla de hechos de calidad.

    Métricas:
        - sample_size: Tamaño de la muestra inspeccionada
        - defects_found: Defectos encontrados
        - defect_rate_pct: Tasa de defectos
        - pass_count: Cantidad de pases
        - fail_count: Cantidad de fallas

    Args:
        quality_checks_df: Tabla limpia de quality_checks
        dim_*: Dimensiones ya construidas

    Returns:
        DataFrame con fact_quality
    """
    # Preparar quality_checks con fecha extraída
    checks = quality_checks_df.withColumn(
        "date_key", F.date_format("check_date", "yyyyMMdd").cast("int")
    )

    # Columna para identificar si paso (booleana)
    checks = checks.withColumn(
        "check_passed", F.when(F.lower(F.col("result")) == "pass", True).otherwise(False)
    )

    # Calcular defect_rate_pct
    checks = checks.withColumn(
        "defect_rate_pct",
        F.when(
            F.col("sample_size") > 0,
            (F.col("defects_found") / F.col("sample_size")) * 100,
        ).otherwise(F.lit(0)),
    )

    # Join con dimensiones para obtener SKs
    # dim_date
    checks = checks.join(
        dim_date.select(F.col("date_sk"), F.col("date_actual")),
        checks["date_key"] == dim_date["date_sk"],
        how="left",
    )

    # dim_production_line
    checks = checks.join(
        dim_production_line.select(F.col("line_sk"), F.col("line_id")),
        on="line_id",
        how="left",
    )

    # dim_order (incluye product_code para el join con dim_product)
    checks = checks.join(
        dim_order.select(
            F.col("order_sk"),
            F.col("order_id").alias("dim_order_id"),
            F.col("product_code").alias("order_product_code"),
        ),
        checks["order_id"] == F.col("dim_order_id"),
        how="left",
    )

    # dim_operator (inspector_id -> operator_sk)
    checks = checks.join(
        dim_operator.select(
            F.col("operator_sk"),
            F.col("operator_id"),
        ),
        checks["inspector_id"] == F.col("operator_id"),
        how="left",
    )

    # dim_product (product_code viene del join con dim_order)
    checks = checks.join(
        dim_product.select(
            F.col("product_sk"),
            F.col("product_code"),
        ),
        F.col("order_product_code") == F.col("product_code"),
        how="left",
    )

    # Generar SK para el fact
    checks = checks.withColumn("quality_sk", F.monotonically_increasing_id())

    # Seleccionar columnas finales según el esquema
    return checks.select(
        "quality_sk",
        "date_sk",
        "line_sk",
        "product_sk",
        "order_sk",
        "operator_sk",
        "sample_size",
        "defects_found",
        "defect_rate_pct",
        "check_passed",
    )
