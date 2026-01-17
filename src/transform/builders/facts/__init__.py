"""
Builders para tablas de hechos del modelo estrella.

Incluye funciones para construir tablas de hechos a partir de datos procesados y dimensiones.
Permite la integración de métricas y eventos clave en el modelo dimensional.
"""

from transform.builders.facts.fact_production import build_fact_production
from transform.builders.facts.fact_quality import build_fact_quality
from transform.builders.facts.fact_sensor_readings import build_fact_sensor_readings
from transform.builders.facts.fact_maintenance import build_fact_maintenance
from transform.builders.facts.fact_alerts import build_fact_alerts

__all__ = [
    "build_fact_production",
    "build_fact_quality",
    "build_fact_sensor_readings",
    "build_fact_maintenance",
    "build_fact_alerts",
]
