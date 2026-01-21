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
```

- Cada fase puede ejecutarse de forma independiente.
- Los datos se almacenan y procesan en S3 (ver configuración).

## Ejecución Orquestada (Producción)

- Se utiliza Apache Airflow para ejecutar el pipeline completo de forma programada. Los detalles 
de ejecución se detallan en [orchestration-docker.md](orchestration-docker.md).

## Estructura de Carpetas (S3)

- `s3a://your_s3_bucket_name_here/raw/`: Datos crudos descargados de la API
- `s3a://your_s3_bucket_name_here/processed/`: Datos limpios y validados
- `s3a://your_s3_bucket_name_here/output/`: Tablas dimensionales y de hechos listas para análisis
- `s3a://your_s3_bucket_name_here/exports/`: Archivos CSV exportados para consumo externo

## Recursos Relacionados

- [etl-docker.md](etl-docker.md): Uso de Docker y pruebas
- [orchestration-docker.md](orchestration-docker.md): Orquestación avanzada y Airflow
- [star-schema.md](star-schema.md): Documentación del esquema estrella alimentado
