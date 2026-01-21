"""
Módulo de utilidades para operaciones de almacenamiento en S3 en el pipeline ETL.

Contexto:
- Rol: Utilidades (Utilities)
- Propósito: Abstrae la lógica de guardado, lectura y manejo de archivos Parquet y CSV en S3 usando pyarrow y boto3.
- Dependencias clave: pyarrow, boto3

Este módulo permite la integración eficiente entre el pipeline ETL y el almacenamiento en la nube.
"""

import os
import tempfile
from typing import List, Dict, Any
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import boto3
from urllib.parse import urlparse


class S3IO:
    """
    Abstracción de operaciones de almacenamiento en S3 para el pipeline ETL.

    Responsabilidad:
    - Guardar y leer archivos Parquet y CSV en S3.
    - Listar y gestionar objetos en buckets S3.
    - Facilitar la interoperabilidad entre pyarrow y S3.

    Uso:
    Instanciar y utilizar para todas las operaciones de I/O con S3 en el pipeline.
    """

    def __init__(self):
        """
        Inicializa el cliente de S3 usando las variables de entorno AWS estándar.
        """
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION"),
        )

    def _parse_s3_path(self, s3_path: str):
        """
        Parsea un path S3 (s3a://bucket/key) y retorna bucket y key.
        """
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    def get_bucket_objects(self, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
        """
        Lista los objetos en un bucket S3 bajo un prefijo dado.

        Args:
            bucket: Nombre del bucket S3.
            prefix: Prefijo para filtrar objetos.

        Returns:
            Lista de diccionarios con metadatos de los objetos.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        objects = []
        for page in page_iterator:
            if "Contents" in page:
                objects.extend(page["Contents"])
        return objects

    def save_parquet(self, payload: List[Dict[str, Any]], s3_path: str) -> str:
        """
        Guarda una lista de diccionarios como Parquet en S3 usando pyarrow.

        Args:
            payload: Lista de diccionarios (registros).
            s3_path: Ruta S3 destino base (debe ser un directorio terminado en /, ej: s3://bucket/raw/tabla/).

        Returns:
            Ruta S3 donde se guardó el archivo.
        """
        table = pa.Table.from_pylist(payload)
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key_prefix = parsed.path.lstrip("/")
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        parquet_filename = f"{timestamp}.parquet"
        final_key = f"{key_prefix}{parquet_filename}"
        final_s3_path = f"s3://{bucket}/{final_key}"
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            pq.write_table(table, tmp.name)
            tmp.flush()
            self.s3.upload_file(tmp.name, bucket, final_key)
        return final_s3_path

    def save_csv(
        self,
        table: pa.Table,
        s3_path: str,
        delimiter: str = ",",
        include_header: bool = True,
    ) -> str:
        """
        Guarda una tabla PyArrow como CSV en S3 usando pyarrow.

        Args:
                table: Tabla PyArrow a escribir.
                s3_path: Ruta S3 destino (s3://bucket/key.csv).
                delimiter: Separador de columnas.
                include_header: Si incluir encabezados.

        Returns:
                Ruta S3 donde se guardó el archivo.
        """
        write_options = pcsv.WriteOptions(
            delimiter=delimiter,
            include_header=include_header,
        )
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            pcsv.write_csv(table, tmp.name, write_options=write_options)
            tmp.flush()
            bucket, key = self._parse_s3_path(s3_path)
            self.s3.upload_file(tmp.name, bucket, key)
        return s3_path

    def get_latest_parquet_path(self, table_name: str, base_path: str) -> str:
        """
        Retorna el path S3 del archivo Parquet más reciente para una tabla en un prefijo base.

        Args:
            table_name: Nombre de la tabla.
            base_path: Prefijo base en S3 (ej: s3://bucket/path)

        Returns:
            Path S3 del archivo Parquet más reciente, o None si no existe.
        """

        parsed = urlparse(base_path)
        bucket = parsed.netloc
        key_prefix = f"{parsed.path.lstrip('/')}/{table_name}/"
        response = self.s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        parquet_files = [
            obj["Key"]
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]
        latest_key = sorted(parquet_files)[-1]
        return f"s3://{bucket}/{latest_key}"

    def read_parquet(self, s3_path: str) -> pa.Table:
        """
        Lee un archivo Parquet desde S3 y retorna un pyarrow Table.

        Args:
            s3_path: Ruta S3 del archivo Parquet.

        Returns:
            pyarrow Table con los datos.
        """

        bucket, key = self._parse_s3_path(s3_path)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            self.s3.download_file(bucket, key, tmp.name)
            table = pq.read_table(tmp.name)
        return table
