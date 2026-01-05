# Docker

## Requisitos

- Docker Desktop instalado y corriendo

## Construcción

```bash
docker-compose build
```

## Contenedor ETL 

Ejecutar el pipeline:

```bash
docker-compose run --rm etl python src/main.py
```

Acceso interactivo (bash):

```bash
docker-compose run --rm etl bash
```

## Contenedor de pruebas

Ejecutar todas las pruebas:

```bash
docker-compose --profile test run --rm test
```

## Limpieza

Detener y eliminar contenedores:

```bash
docker-compose down
```

Eliminar imagen para reconstruir desde cero:

```bash
docker-compose down --rmi local
```

## Notas

- Los volúmenes montan `data/`, `src/` y `config/` desde el host. Los cambios en código no requieren rebuild.
- Los resultados del pipeline se persisten en `data/` local.
- Los reportes de pruebas se guardan en `reports/`.
