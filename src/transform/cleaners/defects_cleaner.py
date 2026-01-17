"""
Módulo de limpieza especializada para la tabla 'defects' en el pipeline ETL.

Contexto:
- Fase: Transformación (Transform)
- Propósito: Implementa reglas de negocio específicas para la limpieza de defectos detectados en el proceso industrial.
- Dependencias clave: PySpark, BaseCleaner

Este módulo extiende la lógica base de limpieza para la tabla de defectos.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class DefectsCleaner(BaseCleaner):
    """
    Cleaner especializado para la tabla 'defects' en la fase de transformación.

    Responsabilidad:
    - Aplicar reglas de negocio específicas para la limpieza de defectos.
    - Derivar columnas y asegurar la calidad de los datos de defectos.
    
    Uso:
    Instanciar con un DataFrame de alertas y llamar a clean().
    """

    def __init__(self, df: DataFrame, id_column: str = "defect_id"):
        super().__init__(df, id_column)

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de valores nulos con reglas de negocio
        específicas para la tabla Defects.

        Reglas:
        1. Eliminar registros con defect_id nulo
        2. Si corrective_action es NULL, asignar "Corrective action not taken"
        """

        # 1. Filtrar registros con id nulo
        df_cleaned = df.filter(F.col(self.id_column).isNotNull())

        # 2. Si corrective_action es NULL, asignar "Corrective action not taken"
        df_cleaned = df_cleaned.withColumn(
            "corrective_action",
            F.when(
                F.col("corrective_action").isNull(),
                F.lit("Corrective action not taken"),
            ).otherwise(F.col("corrective_action")),
        )

        return df_cleaned
