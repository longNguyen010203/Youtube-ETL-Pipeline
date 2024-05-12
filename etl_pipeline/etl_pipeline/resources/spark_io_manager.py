from typing import Any, Union
from datetime import datetime
from dagster import IOManager, InputContext, OutputContext

import os
import polars as pl
from contextlib import contextmanager
from pyspark.sql import SparkSession, DataFrame
from .minio_io_manager import connect_minio


@contextmanager
def create_spark_session(config, appName="Spark IO Manager"):
    # spark = (
    #     SparkSession.builder
    #         .appName(appName)
    #         .master(config["spark_master_url"])
    #         .config("spark.driver.memory", "3g")
    #         .config("spark.executor.memory", "3g")
    #         .config("spark.hadoop.fs.s3a.endpoint", "http://" + config["endpoint_url"])
    #         .config("spark.hadoop.fs.s3a.access.key", str(config["aws_access_key_id"]))
    #         .config("spark.hadoop.fs.s3a.secret.key", str(config["aws_secret_access_key"]))
    #         .config("spark.hadoop.fs.s3a.path.style.access", "true")
    #         .config("spark.hadoop.fs.connection.ssl.enabled", "false")
    #         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #         .getOrCreate()
    # )
    
    spark = (SparkSession.builder.appName(appName) 
                    .master(config["spark_master_url"]) 
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
                    .config("spark.hadoop.fs.s3a.endpoint", "http://" + config["endpoint_url"]) 
                    .config("spark.hadoop.fs.s3a.access.key", str(config["aws_access_key_id"])) 
                    .config("spark.hadoop.fs.s3a.secret.key", str(config["aws_secret_access_key"])) 
                    .getOrCreate()
    )
                                
    try:
        yield spark
    except Exception as e:
        raise f"Error Pyspark: {e}"
    
    
class SparkIOManager(IOManager):
    
    def __init__(self, config) -> None:
        self._config = config

    
    def get_spark_session(self, context) -> SparkSession:
        context.log.info("Return Object SparkSession")
        with create_spark_session(self._config) as spark:
            return spark
        
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), 
            "-".join(context.asset_key.path)
        )
        return key, tmp_file_path
        
    
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        
        if context.has_partition_key:
            start, end = context.asset_partitions_time_window
            # partition_str = context.asset_partition_key
            partition_str = start.strftime("%Y%m")
            context.log.info(f"INFO: {os.path.join(key_name, partition_str)}.pq, {tmp_file_path}")
            key_name, tmp_file_path = os.path.join(key_name, f"{partition_str}.pq"), tmp_file_path
        else:
            context.log.info(f"INFO: {key_name}.pq, {tmp_file_path}")
            key_name, tmp_file_path = f"{key_name}.pq", tmp_file_path
            
        obj.write_parquet(tmp_file_path)
        with connect_minio(self._config) as client:
            try:
                bucket_name = self._config.get("bucket")
                with connect_minio(self._config) as client:
                    # Make bucket if not exist.
                    found = client.bucket_exists(bucket_name)
                    if not found:
                        client.make_bucket(bucket_name)
                    else:
                        print(f"Bucket {bucket_name} already exists")
                    client.fput_object(bucket_name, key_name, tmp_file_path)
                    row_count = len(obj)
                    context.add_output_metadata(
                        {
                            "path": key_name, 
                            "records": row_count, 
                            "tmp": tmp_file_path
                        }
                    )
                    # clean up tmp file
                    os.remove(tmp_file_path)
                    
            except Exception as e:
                raise e
    
    
    def load_input(self, context: InputContext) -> pl.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        
        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            # partition_str = context.asset_partition_key
            partition_str = start.strftime("%Y%m")
            context.log.info(f"INFO: {os.path.join(key_name, partition_str)}.pq, {tmp_file_path}")
            key_name, tmp_file_path = os.path.join(key_name, f"{partition_str}.pq"), tmp_file_path
        else:
            context.log.info(f"INFO: {key_name}.pq, {tmp_file_path}")
            key_name, tmp_file_path = f"{key_name}.pq", tmp_file_path
            
        with connect_minio(self._config) as client:
            try:
                with connect_minio(self._config) as client:
                    # Make bucket if not exist.
                    found = client.bucket_exists(bucket_name)
                    if not found:
                        client.make_bucket(bucket_name)
                    else:
                        print(f"Bucket {bucket_name} already exists")
                    
                    context.log.info(f"INFO -> bucket_name: {bucket_name}")
                    context.log.info(f"INFO -> key_name: {key_name}")
                    context.log.info(f"INFO -> tmp_file_path: {tmp_file_path}")
                    
                    client.fget_object(bucket_name, key_name, tmp_file_path)
                    df = pl.read_parquet(tmp_file_path)
                    
                    return df
                    
            except Exception as e:
                raise e