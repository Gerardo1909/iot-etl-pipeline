"""
Módulo builder para la tabla de hechos 'fact_alerts' en el pipeline ETL.

Contexto:
- Fase: Transformación (Transform)
- Propósito: Construye la tabla de hechos de alertas, integrando eventos de sensores y dimensiones para registrar alertas y tiempos de resolución.
- Dependencias clave: PySpark

Este módulo implementa la lógica de integración y cálculo de métricas de alertas para el modelo dimensional.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_alerts(
    alerts_df: DataFrame,
    dim_date: DataFrame,
    dim_time: DataFrame,
    dim_sensor: DataFrame,
    dim_machine: DataFrame,
    dim_production_line: DataFrame,
    dim_alert_type: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos de alertas.

    Métricas:
        - resolution_time_min: Tiempo hasta resolución en minutos
        - acknowledged_flag: Si fue reconocida
        - severity_level: Nivel de severidad

    Args:
        alerts_df: Tabla limpia de alerts
        dim_*: Dimensiones ya construidas

    Returns:
        DataFrame con fact_alerts
    """
    # Preparar alerts con fecha y hora extraídas
    alerts = alerts_df.withColumn(
        "date_key", F.date_format("triggered_at", "yyyyMMdd").cast("int")
    ).withColumn("hour_key", F.hour("triggered_at"))

    # Calcular resolution_time_min (diferencia entre resolved_at y triggered_at)
    alerts = alerts.withColumn(
        "resolution_time_min",
        F.when(
            F.col("resolved_at").isNotNull(),
            (F.unix_timestamp("resolved_at") - F.unix_timestamp("triggered_at")) / 60,
        ).otherwise(F.lit(None)),
    )

    # Flag de acknowledged
    alerts = alerts.withColumn(
        "acknowledged_flag",
        F.when(F.col("acknowledged_at").isNotNull(), True).otherwise(False),
    )

    # Join con dimensiones para obtener SKs
    # dim_date
    alerts = alerts.join(
        dim_date.select(F.col("date_sk"), F.col("date_actual")),
        alerts["date_key"] == dim_date["date_sk"],
        how="left",
    )

    # dim_time
    alerts = alerts.join(
        dim_time.select(F.col("time_sk"), F.col("hour_of_day")),
        alerts["hour_key"] == dim_time["hour_of_day"],
        how="left",
    )

    # dim_sensor
    alerts = alerts.join(
        dim_sensor.select(
            F.col("sensor_sk"), F.col("sensor_id").alias("dim_sensor_id")
        ),
        alerts["sensor_id"] == F.col("dim_sensor_id"),
        how="left",
    )

    # dim_machine (incluye line_id para derivar line_sk)
    alerts = alerts.join(
        dim_machine.select(
            F.col("machine_sk"),
            F.col("machine_id").alias("dim_machine_id"),
            F.col("line_id"),
        ),
        alerts["machine_id"] == F.col("dim_machine_id"),
        how="left",
    )

    # dim_production_line (line_sk via line_id de machine)
    alerts = alerts.join(
        dim_production_line.select(
            F.col("line_sk"),
            F.col("line_id").alias("dim_line_id"),
        ),
        alerts["line_id"] == F.col("dim_line_id"),
        how="left",
    )

    # dim_alert_type
    alerts = alerts.join(
        dim_alert_type.select(
            F.col("alert_type_sk"),
            F.col("alert_type_name"),
            F.col("severity_level"),
        ),
        (alerts["alert_type"] == F.col("alert_type_name"))
        & (alerts["severity"] == F.col("severity_level")),
        how="left",
    )

    # Generar SK para el fact
    alerts = alerts.withColumn("alert_sk", F.monotonically_increasing_id())

    # Seleccionar columnas finales
    return alerts.select(
        "alert_sk",
        "date_sk",
        "time_sk",
        "sensor_sk",
        "machine_sk",
        "line_sk",
        "alert_type_sk",
        "resolution_time_min",
        "acknowledged_flag",
        F.col("severity").alias("severity_level"),
    )
