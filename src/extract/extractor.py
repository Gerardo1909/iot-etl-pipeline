"""
Módulo con lógica de extracción de API.
"""

from typing import Dict, Any, List, Optional
from extract.http_client import RequestsHttpClient


class Extractor:
    """
    Clase que se encarga de la extracción de información
    dentro del flujo ETL, guardando directamente en S3 usando S3IO (pyarrow+boto3).
    """

    def __init__(
        self,
        http_client: RequestsHttpClient,
        IO_operator,
        raw_data_dir: str,
    ):
        self.client = http_client
        self.IO_operator = IO_operator
        self.raw_data_dir = raw_data_dir

    def extract(self) -> Dict[str, str]:
        """
        Extrae y guarda todas las tablas de interés como Parquet en S3 usando pyarrow.
        Retorna un diccionario con los paths S3 de los archivos guardados.
        """
        payload = self.client.get()
        saved_files = {}

        for table_name in payload.get("tables", {}).keys():
            table_data = self._get_table_from_payload(payload, table_name)
            if table_data:
                s3_path = f"{self.raw_data_dir}/{table_name}/"
                self.IO_operator.save_parquet(payload=table_data, s3_path=s3_path)
                saved_files[table_name] = s3_path

        if not saved_files:
            raise ValueError("No se encontraron tablas para extraer en el payload.")
        return saved_files

    def _get_table_from_payload(
        self, payload: Dict[str, Any], table_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Extrae una tabla específica del payload.
        """
        tables = payload.get("tables", {})
        return tables.get(table_name)


if __name__ == "__main__":
    from utils.s3_io import S3IO
    from config.path_config import get_api_url, RAW_DATA_DIR

    s3_io = S3IO()
    http_client = RequestsHttpClient(url=get_api_url())
    extractor = Extractor(
        http_client=http_client, IO_operator=s3_io, raw_data_dir=RAW_DATA_DIR
    )
    saved_files = extractor.extract()
