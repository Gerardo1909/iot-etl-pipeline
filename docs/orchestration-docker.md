# Orquestación con Airflow y Docker

Esta guía explica cómo orquestar el pipeline ETL usando Apache Airflow y Docker para lograr automatización, monitoreo y escalabilidad.

## Requisitos

- Docker Desktop corriendo
- Imagen ETL construida (ver [etl-docker.md](etl-docker.md))

## Estructura del Proyecto

```
orchestration/
├── docker-compose.yml   # Airflow + Postgres
├── dags/
│   └── iot_etl_dag.py   # DAG del pipeline
├── logs/                # Logs de ejecución
└── plugins/             # Extensiones (vacío)
```

## Configuración Inicial

1. Edita el archivo `.env` en la raíz del proyecto y ajusta las rutas y credenciales necesarias.
2. Inicializa la base de datos y el usuario admin de Airflow:

    ```bash
    $ docker-compose up airflow-init
    ```

3. Levanta los servicios en background:

    ```bash
    $ docker-compose up -d
    ```

4. Accede a la UI de Airflow:
   - URL: http://localhost:8080
   - Usuario: admin (por defecto, cambiar en producción)
   - Password: admin (por defecto, cambiar en producción)

## Ejecución y Monitoreo

- El DAG `iot_etl_dag.py` ejecuta las fases Extract, Transform y Load en contenedores Docker.
- Los logs de cada tarea quedan almacenados en `orchestration/logs/` y pueden consultarse desde la UI.
- El pipeline puede programarse para ejecución periódica (ej. diaria, horaria, etc.).

## Buenas Prácticas y Seguridad

- Cambia las credenciales por defecto antes de usar en producción.
- Revisa los logs y el estado de los DAGs regularmente.
- Mantén las imágenes y dependencias actualizadas.

---

Para detalles sobre el modelo de datos, beneficios y despliegue, consulta:
- [README principal](../README.md)
- [getting-started.md](getting-started.md)
- [etl-docker.md](etl-docker.md)
