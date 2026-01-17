"""
Builders para tablas dimensionales del modelo estrella.

Incluye funciones para construir dimensiones simples, derivadas y generadas.
Permite la generación de tablas de referencia para análisis y joins en el modelo dimensional.
"""

from transform.builders.dimensions.dim_date import build_dim_date
from transform.builders.dimensions.dim_time import build_dim_time
from transform.builders.dimensions.simple_dimensions import (
    SIMPLE_DIMENSIONS,
    SimpleDimensionConfig,
    build_simple_dimension,
)
from transform.builders.dimensions.derived_dimensions import (
    build_dim_product,
    build_dim_alert_type,
    build_dim_maintenance_type,
)

__all__ = [
    "build_dim_date",
    "build_dim_time",
    "SIMPLE_DIMENSIONS",
    "SimpleDimensionConfig",
    "build_simple_dimension",
    "build_dim_product",
    "build_dim_alert_type",
    "build_dim_maintenance_type",
]
