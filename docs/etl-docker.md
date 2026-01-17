# Contenedor ETL: Desarrollo y Pruebas

Esta guía describe cómo utilizar Docker para ejecutar y probar el pipeline ETL de NexusForge Industries de forma aislada y reproducible.

## Requisitos

- Docker Desktop corriendo
- Imagen ETL construida (ver [Guía de Inicio](getting-started.md))

## Construcción de la Imagen

```bash
$ docker-compose build
```

La imagen incluye Python 3.13, PySpark 4.0 y Java 17. Tamaño aproximado: ~2.4GB

## Ejecución de Fases ETL

```bash
# Extract: descarga datos de API → S3/data/raw/
$ docker-compose run --rm etl python -m extract.extractor

# Transform: limpia y modela → S3/data/output/
$ docker-compose run --rm etl python -m transform.transformer

# Load: exporta a CSV → S3/data/exports/
$ docker-compose run --rm etl python -m load.loader
```

- Cada comando ejecuta una fase del pipeline de manera independiente y reproducible.
- Los datos se almacenan y procesan en S3, facilitando la integración con herramientas analíticas.

## Ejecución de Tests

```bash
# Todos los tests con cobertura
$ docker-compose run --rm test

# Tests específicos
$ docker-compose run --rm test pytest tests/unit/extract -v
```

## Beneficios del Enfoque Contenerizado

- Entornos consistentes y portables
- Fácil integración con CI/CD
- Aislamiento de dependencias y recursos

---

Para detalles sobre orquestación avanzada y arquitectura, consulta:
- [orchestration-docker.md](orchestration-docker.md)
- [README principal](../README.md)
