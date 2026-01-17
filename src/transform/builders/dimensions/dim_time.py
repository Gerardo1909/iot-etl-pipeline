"""
Módulo builder para la dimensión 'dim_time' en el pipeline ETL.

Contexto:
- Fase: Transformación (Transform)
- Propósito: Genera la dimensión de tiempo con granularidad horaria, útil para análisis temporal en el modelo dimensional.
- Dependencias clave: PySpark

Este módulo implementa la lógica para construir la tabla de horas sin depender de datos fuente.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_time(spark: SparkSession) -> DataFrame:
    """
    Genera la dimensión de tiempo con granularidad horaria.

    Genera 24 registros (uno por hora del día) con atributos
    útiles para análisis temporal.

    Args:
        spark: SparkSession activa

    Returns:
        DataFrame con la dimensión de tiempo
    """
    # Generar horas del 0 al 23
    df = spark.range(0, 24).withColumnRenamed("id", "hour_of_day")

    # Agregar atributos de tiempo
    df = df.withColumn("time_sk", F.col("hour_of_day").cast("int"))
    df = df.withColumn("minute_of_hour", F.lit(0))  # Granularidad horaria
    df = df.withColumn(
        "time_of_day",
        F.when(F.col("hour_of_day") < 6, "Night")
        .when(F.col("hour_of_day") < 12, "Morning")
        .when(F.col("hour_of_day") < 18, "Afternoon")
        .otherwise("Evening"),
    )
    df = df.withColumn(
        "period_am_pm", F.when(F.col("hour_of_day") < 12, "AM").otherwise("PM")
    )
    df = df.withColumn(
        "hour_band",
        F.concat(
            F.lpad(F.col("hour_of_day"), 2, "0"),
            F.lit(":00 - "),
            F.lpad(F.col("hour_of_day"), 2, "0"),
            F.lit(":59"),
        ),
    )
    df = df.withColumn(
        "is_peak_hour",
        F.when(
            (F.col("hour_of_day").between(6, 9))
            | (F.col("hour_of_day").between(17, 20)),
            True,
        ).otherwise(False),
    )

    # Ordenar columnas según el esquema
    return df.select(
        "time_sk",
        "hour_of_day",
        "minute_of_hour",
        "time_of_day",
        "period_am_pm",
        "hour_band",
        "is_peak_hour",
    )
