"""
Pruebas unitarias para clase S3IO.
"""

import pytest
import pytest_check as check
from unittest.mock import patch, MagicMock
import pyarrow as pa

from utils.s3_io import S3IO


class TestS3IOParseS3Path:
    """
    Pruebas para el método _parse_s3_path.
    """

    def test_parse_s3_path_should_return_bucket_and_key_when_valid_path(self):
        """
        Verifica que _parse_s3_path retorne el bucket y key correctos para un path válido.
        """
        s3io = S3IO()
        bucket, key = s3io._parse_s3_path("s3://my-bucket/path/to/file.parquet")
        check.equal(bucket, "my-bucket")
        check.equal(key, "path/to/file.parquet")

    def test_parse_s3_path_should_handle_s3a_scheme(self):
        """
        Verifica que _parse_s3_path maneje correctamente el esquema s3a://.
        """
        s3io = S3IO()
        bucket, key = s3io._parse_s3_path("s3a://bucket/dir/tabla/")
        check.equal(bucket, "bucket")
        check.equal(key, "dir/tabla/")


class TestS3IOSaveParquet:
    """
    Pruebas para el método save_parquet.
    """

    @patch("utils.s3_io.boto3.client")
    @patch("utils.s3_io.pq.write_table")
    def test_save_parquet_should_upload_file_and_return_path_when_successful(
        self, mock_write_table, mock_boto_client
    ):
        """
        Verifica que save_parquet suba el archivo y retorne el path S3 cuando es exitoso.
        """
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        s3io = S3IO()
        payload = [{"id": 1, "value": 42}]
        s3_path = "s3://bucket/raw/tabla/"
        result = s3io.save_parquet(payload, s3_path)
        check.is_true(result.startswith("s3://bucket/raw/tabla/"))
        check.is_true(result.endswith(".parquet"))
        check.equal(mock_s3.upload_file.call_count, 1)


class TestS3IOGetLatestParquetPath:
    """
    Pruebas para el método get_latest_parquet_path.
    """

    @patch("utils.s3_io.boto3.client")
    def test_get_latest_parquet_path_should_return_latest_when_files_exist(
        self, mock_boto_client
    ):
        """
        Verifica que get_latest_parquet_path retorne el archivo Parquet más reciente si existen archivos.
        """
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "output/tabla/20260101T100000.parquet"},
                {"Key": "output/tabla/20260103T150000.parquet"},
                {"Key": "output/tabla/20260102T120000.parquet"},
            ]
        }
        s3io = S3IO()
        result = s3io.get_latest_parquet_path("tabla", "s3://bucket/output")
        check.equal(result, "s3://bucket/output/tabla/20260103T150000.parquet")

    @patch("utils.s3_io.boto3.client")
    def test_get_latest_parquet_path_should_raise_when_no_files(self, mock_boto_client):
        """
        Verifica que get_latest_parquet_path lance KeyError si no hay archivos Parquet.
        """
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}
        s3io = S3IO()
        with pytest.raises(KeyError):
            s3io.get_latest_parquet_path("tabla", "s3://bucket/output")


class TestS3IOReadParquet:
    """
    Pruebas para el método read_parquet.
    """

    @patch("utils.s3_io.boto3.client")
    @patch("utils.s3_io.pq.read_table")
    def test_read_parquet_should_return_pyarrow_table(
        self, mock_read_table, mock_boto_client
    ):
        """
        Verifica que read_parquet descargue y retorne un objeto pyarrow.Table válido.
        """
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_table = pa.table({"id": [1, 2], "value": [10, 20]})
        mock_read_table.return_value = mock_table
        s3io = S3IO()
        # patch download_file to just write a file
        mock_s3.download_file.side_effect = lambda bucket, key, filename: open(
            filename, "wb"
        ).close()
        result = s3io.read_parquet("s3://bucket/output/tabla/20260103T150000.parquet")
        check.is_instance(result, pa.Table)
        check.equal(result.num_rows, 2)


class TestS3IOGetBucketObjects:
    """
    Pruebas para el método get_bucket_objects.
    """

    @patch("utils.s3_io.boto3.client")
    def test_get_bucket_objects_should_return_all_objects(self, mock_boto_client):
        """
        Verifica que get_bucket_objects retorne todos los objetos bajo el prefijo dado.
        """
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "a"}, {"Key": "b"}]},
            {"Contents": [{"Key": "c"}]},
        ]
        s3io = S3IO()
        result = s3io.get_bucket_objects("bucket", "prefix/")
        check.equal(len(result), 3)
        check.is_true(any(obj["Key"] == "a" for obj in result))
        check.is_true(any(obj["Key"] == "c" for obj in result))
