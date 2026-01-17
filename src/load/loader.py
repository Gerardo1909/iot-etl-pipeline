"""
Módulo principal para la capa de carga para el pipeline ETL.

Contexto:
- Rol: Carga (Load)
- Propósito: Exportar datos procesados desde Parquet a CSV en S3 para su uso final.
- Dependencias clave: pyarrow, boto3

Este módulo contiene la lógica para ejecutar la carga de datos procesados desde Parquet a CSV en S3 para su uso final analítico.
"""

from typing import Dict, List
from config.path_config import OUTPUT_DATA_DIR, EXPORTS_DIR


class Loader:
    """
    Abstracción de operaciones de carga de datos.

    Responsabilidad:
    - Listar las tablas disponibles en el directorio S3 de output.
    - Leer archivos Parquet y exportarlos como CSV en S3 usando pyarrow y boto3.

    Uso:
    Instanciar con las dependencias IO_operator (S3IO), output_data_dir, exports_dir, y
    utilizar el método load() para ejecutar la carga.
    """

    def __init__(
        self,
        IO_operator,
        output_data_dir: str = OUTPUT_DATA_DIR,
        exports_dir: str = EXPORTS_DIR,
    ):
        self.output_data_dir = output_data_dir
        self.exports_dir = exports_dir
        self.IO_operator = IO_operator

    def _list_tables(self) -> List[str]:
        """
        Lista las tablas disponibles en el directorio S3 de output.
        Retorna los nombres de subcarpetas (tablas) bajo output_data_dir.
        """
        bucket, prefix = self.IO_operator._parse_s3_path(self.output_data_dir)
        objects = self.IO_operator.get_bucket_objects(bucket, prefix)
        table_names = set()
        for obj in objects:
            key = obj["Key"]
            relative_path = key[len(prefix) :].lstrip("/")
            parts = relative_path.split("/")
            if len(parts) > 1:
                table_names.add(parts[0])
        return list(table_names)

    def load(self) -> Dict[str, str]:
        """
        Lee todas las tablas Parquet presentes en output_data_dir y las exporta como CSV a exports_dir en S3 usando pyarrow.
        Returns:
            Diccionario {nombre_tabla: path S3 de exportación}.
        """
        tables = self._list_tables()
        results = {}
        for table_name in tables:
            parquet_path = self.IO_operator.get_latest_parquet_path(
                table_name, self.output_data_dir
            )
            if not parquet_path:
                raise ValueError(
                    f"No se encontró archivo Parquet para la tabla {table_name} en {self.output_data_dir}"
                )
            export_path = f"{self.exports_dir}/{table_name}.csv"
            table = self.IO_operator.read_parquet(parquet_path)
            self.IO_operator.save_csv(table, export_path)
            results[table_name] = export_path
        return results


if __name__ == "__main__":
    from utils.s3_io import S3IO

    s3_io = S3IO()
    loader = Loader(IO_operator=s3_io)
    results = loader.load()
