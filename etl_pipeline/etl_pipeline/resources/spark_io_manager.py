from typing import Any
from dagster import IOManager, InputContext, OutputContext

from contextlib import contextmanager
from pyspark.sql import SparkSession, DataFrame


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
    
    spark = SparkSession.builder.appName(appName) \
                                .master(config["spark_master_url"]) \
                                .getOrCreate()
                                
    try:
        yield spark
    except Exception as e:
        raise f"Error Pyspark: {e}"
    
    
class SparkIOManager(IOManager):
    
    def __init__(self, config) -> None:
        self._config = config
        
    # def get_data_from_minio(self, context):
        
    #     file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)
        
    #     with create_spark_session(self._config) as spark:
    #         spark_df = spark.read.parquet()
    
    def get_spark_session(self, context) -> SparkSession:
        
        context.log.info("Return Object SparkSession")
        with create_spark_session(self._config) as spark:
            return spark
        
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        pass
    
    def load_input(self, context: InputContext) -> Any:
        pass