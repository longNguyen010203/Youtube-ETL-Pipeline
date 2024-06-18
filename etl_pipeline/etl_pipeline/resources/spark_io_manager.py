from typing import Any, Union
from datetime import datetime
from dagster import IOManager, InputContext, OutputContext

import os
import polars as pl
import pandas as pd
from contextlib import contextmanager
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from .minio_io_manager import connect_minio


@contextmanager
def create_spark_session(config, appName=None):
    spark = (
        SparkSession.builder.appName(appName)
            .master("spark://spark-master:7077")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            # .config("spark.cores.max", "4")
            # .config("spark.executor.cores", "4")
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", "http://" + config["endpoint_url"])
            .config("spark.hadoop.fs.s3a.access.key", str(config["aws_access_key_id"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["aws_secret_access_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
    )

    try:
        yield spark
    except Exception as e:
        raise f"Error Pyspark: {e}"
    
    
class SparkIOManager(IOManager):
    
    def __init__(self, config) -> None:
        self._config = config

    
    def get_spark_session(self, context, appName=None) -> SparkSession:
        with create_spark_session(self._config, appName) as spark:
            context.log.info("Return Object SparkSession")
            return spark
        
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), 
            "-".join(context.asset_key.path)
        )
        return key, tmp_file_path
    
    
    def handle_output(self, context: OutputContext, obj: DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        ## ====>
        file_path = "s3a://lakehouse/" + key_name
        context.log.info(f"file_path: {file_path}")
        context.log.info(f"key_name: {key_name}")
        
        if context.has_partition_key:
            start, end = context.asset_partitions_time_window
            # partition_str = context.asset_partition_key
            partition_str = start.strftime("%Y%m")
            context.log.info(f"INFO: {os.path.join(key_name, partition_str)}.parquet, {tmp_file_path}")
            key_name, tmp_file_path = os.path.join(key_name, f"{partition_str}.parquet"), tmp_file_path
        else:
            context.log.info(f"INFO: {key_name}.parquet, {tmp_file_path}")
            key_name, tmp_file_path = f"{key_name}.parquet", tmp_file_path
        
        
        obj.write.mode('overwrite').parquet(tmp_file_path)
 
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
                    row_count = obj.count()
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
    
    
    def load_input(self, context: InputContext) -> DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        
        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            # partition_str = context.asset_partition_key
            partition_str = start.strftime("%Y%m")
            context.log.info(f"INFO: {os.path.join(key_name, partition_str)}.parquet, {tmp_file_path}")
            key_name, tmp_file_path = os.path.join(key_name, f"{partition_str}.parquet"), tmp_file_path
        else:
            context.log.info(f"INFO: {key_name}.parquet, {tmp_file_path}")
            key_name, tmp_file_path = f"{key_name}.parquet", tmp_file_path
            
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
                    
                    spark: SparkSession = self.get_spark_session(self, appName="Read-Parquet")
                    df = spark.read.parquet(tmp_file_path)
                    
                    return df
                    
            except Exception as e:
                raise e