import polars as pl
from pyspark.sql import SparkSession, DataFrame

from dagster import asset, AssetExecutionContext
from dagster import Output, MetadataValue
from dagster import AssetIn

from ..resources.spark_io_manager import create_spark_session


GROUP_NAME = "bronze"

@asset(
    ins={
        "videoCategory_trending_data": AssetIn(
            key_prefix=["bronze", "youtube"]
        )
    },
    name="bronze_youtube_trending_data",
    required_resource_keys={"spark_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "youtube"],
    compute_kind="PySpark",
    group_name=GROUP_NAME
)
def silver_videoCategory_clean(context: AssetExecutionContext,
                               videoCategory_trending_data: pl.DataFrame) -> Output[DataFrame]:
    """ 
        Clean 'videoCategory_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    spark_df: DataFrame = spark.createDataFrame(videoCategory_trending_data)
    
    return Output(
        value=spark_df,
        metadata={
            "File Name": MetadataValue.text("silver_videoCategory_clean.pq"),
            "Number Columns": MetadataValue.int(len(spark_df.columns)),
            "Number Records": MetadataValue.int(spark_df.count())
        }
    )