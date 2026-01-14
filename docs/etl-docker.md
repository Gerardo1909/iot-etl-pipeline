# Contenedor ETL

Imagen Docker con PySpark para ejecutar el pipeline ETL de forma aislada.

## Requisitos

- Docker Desktop corriendo

## Construcción

```bash
docker-compose build
```

> La imagen incluye Python 3.13, PySpark 4.0 y Java 17. Tamaño aproximado: ~2.4GB

## Comandos

### Ejecutar fases individuales

```bash
# Extract: descarga datos de API → data/raw/
docker-compose run --rm etl python -m extract.extractor

# Transform: limpia y modela → data/output/
docker-compose run --rm etl python -m transform.transformer

# Load: exporta a CSV → data/exports/
docker-compose run --rm etl python -m load.loader
```

### Ejecutar tests

```bash
# Todos los tests con cobertura
docker-compose run --rm test

# Tests específicos
docker-compose run --rm test pytest tests/unit/extract -v
```

### Acceso interactivo

```bash
# Shell bash dentro del contenedor
docker-compose run --rm etl bash

# Consola Python/PySpark
docker-compose run --rm etl python
```

## Limpieza

```bash
# Detener contenedores
docker-compose down

# Eliminar imagen (forzar rebuild)
docker-compose down --rmi local

# Limpiar todo (contenedores + volúmenes + imágenes)
docker-compose down -v --rmi local
```

## Volúmenes montados

| Host | Contenedor | Modo |
|------|------------|------|
| `data/` | `/app/data` | lectura/escritura |
| `src/` | `/app/src` | solo lectura |
| `config/` | `/app/config` | solo lectura |
| `.env` | `/app/.env` | solo lectura |

> Los cambios en `src/` y `config/` se reflejan sin rebuild.

## Estructura de datos

```
data/
├── raw/        # Bronze: datos crudos de la API (Parquet)
├── processed/  # Silver: datos limpios (Parquet)
├── output/     # Gold: modelo dimensional (Parquet)
└── exports/    # Exportaciones finales (CSV)
```
