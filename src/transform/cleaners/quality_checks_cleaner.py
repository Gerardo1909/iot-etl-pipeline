"""
Módulo de limpieza especializada para la tabla 'quality_checks' en el pipeline ETL.

Contexto:
- Fase: Transformación (Transform)
- Propósito: Implementa reglas de negocio específicas para la limpieza de registros de quality checks.
- Dependencias clave: PySpark, BaseCleaner

Este módulo extiende la lógica base de limpieza para la tabla de quality checks.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class QualityChecksCleaner(BaseCleaner):
    """
    Cleaner especializado para la tabla 'quality_checks' en la fase de transformación.

    Responsabilidad:
    - Aplicar reglas de negocio específicas para la limpieza de registros de quality checks.
    - Derivar columnas y asegurar la calidad de los datos de quality checks.

    Uso:
    Instanciar con un DataFrame de registros de quality checks y llamar a clean().
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
