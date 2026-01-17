# Guía de Inicio Rápido

Esta guía explica cómo ejecutar el pipeline ETL de IoT, tanto en desarrollo como en producción.

## Prerrequisitos

- Docker Desktop instalado y corriendo
- Archivo `.env` configurado con credenciales y rutas (ver `.env.example`)

## Ejecución Manual (Desarrollo)

Ideal para desarrollo, debugging y pruebas locales.

```bash
# 1. Construir imagen
$ docker-compose build

# 2. Ejecutar fases del pipeline
$ docker-compose run --rm etl python -m extract.extractor
$ docker-compose run --rm etl python -m transform.transformer
$ docker-compose run --rm etl python -m load.loader

# 3. Verificar resultados
$ ls data/exports/
```

- Cada fase puede ejecutarse de forma independiente.
- Los datos se almacenan y procesan en S3 (ver configuración).

## Ejecución Orquestada (Producción)

- Utiliza Apache Airflow para ejecutar el pipeline completo de forma programada.
- Ver detalles y mejores prácticas en [orchestration-docker.md](orchestration-docker.md).

## Estructura de Carpetas

- `data/raw/`: Datos crudos descargados de la API
- `data/processed/`: Datos limpios y validados
- `data/output/`: Tablas dimensionales y de hechos listas para análisis
- `data/exports/`: Archivos CSV exportados para consumo externo

## Recursos Relacionados

- [etl-docker.md](etl-docker.md): Uso de Docker y pruebas
- [orchestration-docker.md](orchestration-docker.md): Orquestación avanzada y Airflow

---

Para detalles sobre el modelo dimensional, beneficios y arquitectura, consulta el [README principal](../README.md).
