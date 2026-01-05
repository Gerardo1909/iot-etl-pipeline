"""
Módulo que se encarga de la limpieza específica para la tabla 'QualityChecks'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class QualityChecksCleaner(BaseCleaner):
    """
    Clase específica para la limpieza de la tabla 'QualityChecks'.
    """

    def __init__(self, df: DataFrame, id_column: str = "check_id"):
        super().__init__(df, id_column)

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de valores nulos con reglas de negocio
        específicas para la tabla QualityChecks.

        Reglas:
        1. Eliminar registros con check_id nulo
        2. Si notes es NULL, asignar "No notes provided"
        """

        # 1. Filtrar registros con id nulo
        df_cleaned = df.filter(F.col(self.id_column).isNotNull())

        # 2. Si notes es NULL, asignar "No notes provided"
        df_cleaned = df_cleaned.withColumn(
            "notes",
            F.when(
                F.col("notes").isNull(),
                F.lit("No notes provided"),
            ).otherwise(F.col("notes")),
        )

        return df_cleaned
