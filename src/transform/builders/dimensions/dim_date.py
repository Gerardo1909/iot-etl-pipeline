"""
Generador de la dimensión dim_date.

Esta dimensión se genera a partir de un rango de fechas,
no se extrae de datos existentes.
"""

from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_date(
    spark: SparkSession,
    start_date: date = date(2020, 1, 1),
    end_date: date = date(2030, 12, 31),
) -> DataFrame:
    """
    Genera la dimensión de fechas con atributos útiles para análisis.

    Args:
        spark: SparkSession activa
        start_date: Fecha inicial del rango
        end_date: Fecha final del rango

    Returns:
        DataFrame con la dimensión de fechas
    """
    # Generar secuencia de fechas
    df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_date}'), 
            to_date('{end_date}'), 
            interval 1 day
        )) as date_actual
    """)

    # Agregar atributos de fecha
    df = df.withColumn("date_sk", F.date_format("date_actual", "yyyyMMdd").cast("int"))
    df = df.withColumn("day_of_week", F.dayofweek("date_actual"))
    df = df.withColumn("day_name", F.date_format("date_actual", "EEEE"))
    df = df.withColumn("week_of_year", F.weekofyear("date_actual"))
    df = df.withColumn("month_number", F.month("date_actual"))
    df = df.withColumn("month_name", F.date_format("date_actual", "MMMM"))
    df = df.withColumn("quarter", F.quarter("date_actual"))
    df = df.withColumn("year", F.year("date_actual"))
    df = df.withColumn(
        "is_weekend",
        F.when(F.dayofweek("date_actual").isin(1, 7), True).otherwise(False),
    )
    df = df.withColumn(
        "fiscal_period",
        F.concat(
            F.lit("FY"), F.year("date_actual"), F.lit("-Q"), F.quarter("date_actual")
        ),
    )

    # Determinar si es feriado (simplificado: solo fines de semana por ahora)
    # En producción, esto vendría de una tabla de feriados
    df = df.withColumn("is_holiday", F.lit(False))

    # Ordenar columnas
    return df.select(
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
    )
