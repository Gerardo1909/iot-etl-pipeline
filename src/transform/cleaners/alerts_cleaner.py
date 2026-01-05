"""
Módulo que se encarga de la limpieza específica para la tabla 'Alerts'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class AlertsCleaner(BaseCleaner):
    """
    Clase específica para la limpieza de la tabla 'Alerts'.
    """

    def __init__(self, df: DataFrame, id_column: str = "alert_id"):
        super().__init__(df, id_column)

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de valores nulos con reglas de negocio
        específicas para la tabla Alerts.

        Reglas:
        1. Eliminar registros con alert_id nulo
        2. Si acknowledged_at es NULL pero resolved_at tiene fecha,
           se asume acknowledged_at = resolved_at
        3. Si resolved_by es NULL, se asigna "unknown"
        4. Se crea columna derivada alert_status para indicar el estado
        """

        # 1. Filtrar registros con id nulo
        df_cleaned = df.filter(F.col(self.id_column).isNotNull())

        # 2. Si acknowledged_at es NULL pero resolved_at tiene fecha,
        #    asumimos que acknowledged_at = resolved_at
        df_cleaned = df_cleaned.withColumn(
            "acknowledged_at",
            F.when(
                F.col("acknowledged_at").isNull() & F.col("resolved_at").isNotNull(),
                F.col("resolved_at"),
            ).otherwise(F.col("acknowledged_at")),
        )

        # 3. Si resolved_by es NULL, asignar "unknown"
        df_cleaned = df_cleaned.withColumn(
            "resolved_by",
            F.when(F.col("resolved_by").isNull(), F.lit("unknown")).otherwise(
                F.col("resolved_by")
            ),
        )

        # 4. Crear columna derivada alert_status
        df_cleaned = df_cleaned.withColumn(
            "alert_status",
            F.when(F.col("resolved_at").isNotNull(), F.lit("resolved"))
            .when(
                F.col("acknowledged_at").isNotNull() & F.col("resolved_at").isNull(),
                F.lit("acknowledged"),
            )
            .otherwise(F.lit("pending")),
        )

        return df_cleaned
