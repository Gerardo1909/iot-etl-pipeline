# Esquema estrella pensado para este problema

Se proporciona una descripción detallada de las tablas dimensionales y de hechos producidas por el pipeline ETL, su granularidad, claves, tipos básicos y recomendaciones operativas.

## Tablas de dimensiones

- **dim_date**
	- PK: `date_sk` (YYYYMMDD)
	- Columnas: `date_actual`, `day_of_week`, `day_name`, `week_of_year`, `month_number`, `month_name`, `quarter`, `year`, `is_weekend`, `is_holiday`, `fiscal_period`
	- Granularidad: 1 fila/día
	- Uso: agregaciones temporales, filtros fiscales y ventanas móviles.

- **dim_time**
	- PK: `time_sk`
	- Columnas: `hour_of_day`, `minute_of_hour`, `time_of_day`, `period_am_pm`, `hour_band`, `is_peak_hour`
	- Granularidad: por hora/minuto según `time_sk` (dataset actual muestra 24 horas por día)
	- Uso: análisis por franjas horarias y correlación con picos de actividad.

- **dim_factory**
	- PK: `factory_sk`
	- Columnas: `factory_id`, `factory_name`, `location`, `manager_name`, `total_area_sqm`
	- Granularidad: 1 fila/planta
	- Uso: segmentación y agregación por planta.

- **dim_production_line**
	- PK: `line_sk`
	- Columnas: `line_id`, `factory_id`, `line_name`, `line_type`, `installed_date`, `status`
	- Granularidad: 1 fila/linea de producción
	- Uso: análisis por línea (downtime, rendimiento, mantenimiento).

- **dim_machine**
	- PK: `machine_sk`
	- Columnas: `machine_id`, `line_id`, `machine_name`, `machine_type`, `manufacturer`, `model`, `serial_number`, `installation_date`, `operating_hours`
	- Granularidad: 1 fila/máquina
	- Uso: trazabilidad de eventos y mantenimiento predictivo.

- **dim_product**
	- PK: `product_sk`
	- Columnas: `product_code`, `product_name`, `unit_of_measure`, `priority_default`
	- Granularidad: 1 fila/producto
	- Uso: análisis por SKU, yield y prioridades.

- **dim_order**
	- PK: `order_sk`
	- Columnas: `order_id`, `product_code`, `quantity_ordered`, `start_date`, `target_end_date`, `priority`, `status`
	- Granularidad: 1 fila/orden
	- Uso: relacionar producción con órdenes y medir cumplimiento.

- **dim_sensor**
	- PK: `sensor_sk`
	- Columnas: `sensor_id`, `sensor_name`, `sensor_type`, `unit`, `min_threshold`, `max_threshold`, `is_active`
	- Granularidad: 1 fila/sensor
	- Uso: contexto para lecturas y reglas de anomalía.

- **dim_shift**
	- PK: `shift_sk`
	- Columnas: `shift_id`, `shift_name`, `start_time`, `end_time`
	- Granularidad: 1 fila/turno
	- Uso: análisis por turno (Mañana/Tarde/Noche).

- **dim_operator**
	- PK: `operator_sk`
	- Columnas: `operator_id`, `employee_code`, `certification_level`, `hire_date`
	- Granularidad: 1 fila/operador
	- Uso: atribución de tareas y análisis por habilidades.

- **dim_alert_type**
	- PK: `alert_type_sk`
	- Columnas: `alert_type_name`, `severity_level`, `category`, `requires_ack`
	- Uso: clasificar alertas y priorizar respuestas.

- **dim_maintenance_type**
	- PK: `maint_type_sk`
	- Columnas: `maint_type_name`
	- Uso: agrupar registros de mantenimiento por tipo (preventive, corrective, etc.)



## Tablas de hechos

- **fact_production**
	- PK: `production_sk`
	- FK: `date_sk`, `factory_sk`, `line_sk`, `product_sk`, `shift_sk`, `order_sk`
	- Medidas: `units_produced`, `units_defective`, `downtime_minutes`, `efficiency_percentage`, `target_units`, `cycle_time_avg`
	- Granularidad: registro por unidad/orden/turno/linea según generación; tabla central para KPIs de producción.
	- Recomendación: particionar por `date_sk` y mantener metadatos de versionado si se re-procesan.

- **fact_sensor_readings**
	- PK: `sensor_reading_sk`
	- FK: `date_sk`, `time_sk`, `sensor_sk`, `machine_sk`, `line_sk`
	- Medidas: `timestamp`, `value`, `is_anomaly`, `quality_score`
	- Granularidad: lectura de sensor por timestamp (alta cardinalidad)
	- Recomendación: retención corta para detalle (ej. 90 días) + rollups diarios para histórico.

- **fact_alerts**
	- PK: `alert_sk`
	- FK: `date_sk`, `time_sk`, `sensor_sk`, `machine_sk`, `line_sk`, `alert_type_sk`
	- Medidas: `resolution_time_min`, `acknowledged_flag`, `severity_level`
	- Granularidad: evento de alerta por sensor/tiempo
	- Uso: SLAs, tiempos de respuesta y correlación con fallos.

- **fact_maintenance**
	- PK: `maintenance_sk`
	- FK: `date_sk`, `machine_sk`, `line_sk`, `maintenance_type_sk`, `operator_sk`
	- Medidas: `cost_amount`, `downtime_hours`, `labor_hours`
	- Granularidad: registro por intervención de mantenimiento
	- Uso: análisis de costos, efectividad y planificación de recursos.

- **fact_quality**
	- PK: `quality_sk`
	- FK: `date_sk`, `line_sk`, `product_sk`, `order_sk`, `operator_sk`
	- Medidas: `sample_size`, `defects_found`, `defect_rate_pct`, `check_passed`
	- Granularidad: muestreo de control de calidad por lote/orden
	- Uso: indicadores de calidad, control estadístico y alertas de lote.

## Joins típicos

A continuación se incluyen consultas ejemplo, su propósito y notas de uso/optimización. Estos ejemplos sirven como plantilla para casos analíticos comunes.

1) Producción por planta y línea (KPIs)

**Propósito**: obtener KPIs agregados (unidades, defectos, eficiencia) por planta y línea en un periodo anual.

```sql
SELECT d.year, f.factory_name, p.line_sk,
	   SUM(p.units_produced) AS total_units,
	   SUM(p.units_defective) AS total_defects,
	   AVG(p.efficiency_percentage) AS avg_eff
FROM fact_production p
JOIN dim_date d ON p.date_sk = d.date_sk
JOIN dim_factory f ON p.factory_sk = f.factory_sk
WHERE d.year = 2025
GROUP BY d.year, f.factory_name, p.line_sk;
```

**Notas**: particionar `fact_production` por `date_sk` y filtrar por año/mes reduce costes. Útil para dashboards ejecutivos y análisis de OEE.

2) Correlación telemetría → defectos (ventana temporal)

**Propósito**: correlacionar lecturas de sensores con la incidencia de defectos en una ventana temporal, útil para detección de causas raíz.

```sql
SELECT d.date_actual, t.hour_of_day, m.machine_name, AVG(s.value) AS avg_sensor,
	   SUM(p.units_defective) AS defects
FROM fact_sensor_readings s
JOIN dim_time t ON s.time_sk = t.time_sk
JOIN dim_date d ON s.date_sk = d.date_sk
JOIN dim_machine m ON s.machine_sk = m.machine_sk
LEFT JOIN fact_production p ON p.date_sk = s.date_sk AND p.line_sk = s.line_sk
WHERE d.date_actual BETWEEN '2025-01-01' AND '2025-01-07'
GROUP BY d.date_actual, t.hour_of_day, m.machine_name;
```

**Notas**: `fact_sensor_readings` es de alta cardinalidad; agregar filtros de `date_sk`/`time_sk` y agregar rollups previos mejora rendimiento.

3) SLA de alertas: tiempo medio de resolución por tipo y severidad

**Propósito**: medir cumplimiento de SLA por tipo de alerta y priorizar recursos de respuesta.

```sql
SELECT at.alert_type_name, at.severity_level,
	   COUNT(a.alert_sk) AS alert_count,
	   AVG(a.resolution_time_min) AS mean_resolution_min
FROM fact_alerts a
JOIN dim_alert_type at ON a.alert_type_sk = at.alert_type_sk
JOIN dim_date d ON a.date_sk = d.date_sk
WHERE d.year = 2025
GROUP BY at.alert_type_name, at.severity_level
ORDER BY mean_resolution_min DESC;
```

**Notas**: usar `acknowledged_flag` para segmentar alertas no atendidas; particionar `fact_alerts` por `date_sk` ayuda en queries históricas.

4) Coste de mantenimiento por máquina y tipo

**Propósito**: analizar gasto y impacto en disponibilidad por máquina o por tipo de mantenimiento.

```sql
SELECT m.machine_name, mt.maint_type_name,
	   COUNT(mn.maintenance_sk) AS events,
	   SUM(mn.cost_amount) AS total_cost,
	   SUM(mn.downtime_hours) AS total_downtime
FROM fact_maintenance mn
JOIN dim_machine m ON mn.machine_sk = m.machine_sk
JOIN dim_maintenance_type mt ON mn.maintenance_type_sk = mt.maint_type_sk
WHERE mn.date_sk BETWEEN 20250101 AND 20251231
GROUP BY m.machine_name, mt.maint_type_name
ORDER BY total_cost DESC;
```

**Notas**: normalizar `cost_amount` por moneda/periodo si aplica; combinar con `operating_hours` de `dim_machine` para costes por hora operativa.

5) Calidad por orden/sku: tasa de defectos y gravedad

**Propósito**: identificar órdenes y SKUs con problemas de calidad para acciones correctivas.

```sql
SELECT o.order_id, p.product_code,
	   SUM(q.defects_found) AS total_defects,
	   AVG(q.defect_rate_pct) AS avg_defect_rate,
	   SUM(CASE WHEN q.check_passed = false THEN 1 ELSE 0 END) AS failed_checks
FROM fact_quality q
JOIN dim_order o ON q.order_sk = o.order_sk
JOIN dim_product p ON q.product_sk = p.product_sk
WHERE q.date_sk BETWEEN 20250101 AND 20251231
GROUP BY o.order_id, p.product_code
HAVING AVG(q.defect_rate_pct) > 5
ORDER BY avg_defect_rate DESC;
```

**Notas**: adaptar umbrales (`> 5`%) a la definición de negocio; combinar con `fact_production` para ver impacto en unidades perdidas.

6) Sensores con más anomalías por línea (priorizar inspecciones)

**Propósito**: localizar sensores/zonas con mayor frecuencia de anomalías para mantenimiento predictivo.

```sql
SELECT s.sensor_id, m.line_id, m.machine_name,
	   COUNT(*) AS anomaly_count,
	   AVG(fr.quality_score) AS avg_quality
FROM fact_sensor_readings fr
JOIN dim_sensor s ON fr.sensor_sk = s.sensor_sk
JOIN dim_machine m ON fr.machine_sk = m.machine_sk
WHERE fr.is_anomaly = true AND fr.date_sk BETWEEN 20250101 AND 20251231
GROUP BY s.sensor_id, m.line_id, m.machine_name
ORDER BY anomaly_count DESC
LIMIT 50;
```

**Notas**: útil para priorizar reemplazos o recalibraciones; incluir `min_threshold`/`max_threshold` de `dim_sensor` para investigar falsos positivos.

### Consejos generales de optimización

- Siempre filtrar por `date_sk` o particiones antes de unir tablas de hechos.
- Prefiltrar dimensiones (p. ej. `dim_machine` con subset de máquinas) reduce costos en joins con hechos grandes.
- Para análisis complejos, considerar materializar vistas agregadas (diarias/semanales) para acelerar dashboards.
