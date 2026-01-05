"""
Módulo que define la estructura base para la fase de limpieza.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class BaseCleaner:
    """
    Clase base para la fase de limpieza en el proceso de transformación de datos.
    """
    
    def __init__(self, df: DataFrame, id_column: str):
        self.df = df
        self.id_column = id_column

    def clean(self) -> DataFrame:
        """
        Ejecuta el pipeline de limpieza y devuelve un nuevo DataFrame.
        """
        df = self.df
        df = self._handle_duplicates(df)
        df = self._handle_nulls(df)
        df = self._standardize_types(df)
        df = self._apply_business_rules(df)
        return df
    
    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """p
        Implementa lógica de manejo de valores nulos con lógica de negocio
        específica de la tabla.
        """
        return df.filter(F.col(self.id_column).isNotNull())

    def _handle_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de duplicados.
        """
        return df.dropDuplicates()

    def _standardize_types(self, df: DataFrame) -> DataFrame:
        """
        Implementa proceso de estandarización de tipos de datos.
        """
        return df

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de reglas de negocio para tablas que lo requieran.
        """
        return df
        
