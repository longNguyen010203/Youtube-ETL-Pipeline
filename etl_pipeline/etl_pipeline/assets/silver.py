import polars as pl
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType

from dagster import asset, AssetExecutionContext
from dagster import Output, MetadataValue
from dagster import AssetIn

from ..partitions import monthly_partitions


GROUP_NAME = "bronze"

@asset(
    ins={
        "videoCategory_trending_data": AssetIn(
            key_prefix=["bronze", "youtube"]
        )
    },
    name="silver_videoCategory_clean",
    required_resource_keys={"spark_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "youtube"],
    compute_kind="PySpark",
    group_name=GROUP_NAME
)
def silver_videoCategory_clean(context: AssetExecutionContext,
                               videoCategory_trending_data: pl.DataFrame) -> Output[pl.DataFrame]:
    """ 
        Clean 'videoCategory_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    spark_df: DataFrame = spark.createDataFrame(videoCategory_trending_data.to_pandas())
    spark_df = spark_df.withColumn("categoryId", spark_df["categoryId"].cast(IntegerType()))
    spark_df = spark_df.orderBy(spark_df["categoryId"])
    polars_df = pl.DataFrame(spark_df.toPandas())
    context.log.info("Get Object Spark Session")
    context.log.info("Convert polars dataframe to pyspark dataframe")
    context.log.info("Sort pyspark dataframe by categoryId column")
    
    return Output(
        value=polars_df,
        metadata={
            "File Name": MetadataValue.text("silver_videoCategory_clean.pq"),
            "Number Columns": MetadataValue.int(polars_df.shape[1]),
            "Number Records": MetadataValue.int(polars_df.shape[0])
        }
    )
    
    
@asset(
    ins={
        "linkVideos_trending_data": AssetIn(
            key_prefix=["bronze", "youtube"]
        )
    },
    deps=["bronze_youtube_trending_data"],
    name="silver_linkVideos_clean",
    required_resource_keys={
        "spark_io_manager",
        "youtube_io_manager"
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "youtube"],
    compute_kind="PySpark",
    group_name=GROUP_NAME
)
def silver_linkVideos_clean(context: AssetExecutionContext,
                            linkVideos_trending_data: pl.DataFrame) -> Output[pl.DataFrame]:
    """ 
        Clean 'linkVideos_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    pl_data: pl.DataFrame = context.resources.youtube_io_manager.get_DataFrame(context, "video_id")
    linkVideos: DataFrame = spark.createDataFrame(linkVideos_trending_data.to_pandas())
    trending: DataFrame = spark.createDataFrame(pl_data.to_pandas())
    linkVideos.createOrReplaceTempView("linkVideos")
    trending.createOrReplaceTempView("trending")
    context.log.info("Convert polars dataframe to pyspark dataframe")
    context.log.info("Convert pyspark dataframe to View in SQL query")
    query = """ 
                SELECT t.*
                FROM linkVideos lv RIGHT JOIN trending t
                    on lv.videoId = t.video_id
            """
    spark_df = spark.sql(query)
    spark_df = spark_df.toPandas()
    spark_df = pl.DataFrame(spark_df)
    
    return Output(
        value=spark_df,
        metadata={
            "File Name": MetadataValue.text("silver_linkVideos_clean.pq"),
            "Number Columns": MetadataValue.int(spark_df.shape[1]),
            "Number Records": MetadataValue.int(spark_df.shape[0])
        }
    )
    
    
@asset(
    ins={
        "bronze_youtube_trending_data": AssetIn(
            key_prefix=["bronze", "youtube"]
        )
    },
    # deps=["bronze_youtube_trending_data"],
    name="silver_trending_clean",
    required_resource_keys={
        "spark_io_manager",
        "youtube_io_manager"
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "youtube"],
    partitions_def=monthly_partitions,
    compute_kind="PySpark",
    group_name=GROUP_NAME
)
def silver_trending_clean(context: AssetExecutionContext,
                          bronze_youtube_trending_data: pl.DataFrame) -> Output[pl.DataFrame]:
    """ 
        Clean 'bronze_youtube_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    spark_df: DataFrame = spark.createDataFrame(bronze_youtube_trending_data)
    
    
    return Output(
        value=spark_df,
        metadata={
            "Records": MetadataValue.int(spark_df.shape[0]),
            "Columns": MetadataValue.int(spark_df.shape[1])
        }
    )
    

@asset(
    required_resource_keys={"spark_io_manager"}
)
def read_file_minio(context: AssetExecutionContext) -> Output[DataFrame]:
    
    path = "s3a://lakehouse/bronze/youtube/linkVideos_trending_data.pq"
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    df = spark.read.parquet(path)
    
    return Output(
        value=df,
        metadata={
            "records": MetadataValue.int(df.count()),
            "Columns": MetadataValue.int(len(df.columns))
        }
    )
    