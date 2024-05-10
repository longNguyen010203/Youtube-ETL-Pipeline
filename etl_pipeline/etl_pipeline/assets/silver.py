import polars as pl
from pyspark.sql import SparkSession, DataFrame

from dagster import asset, AssetExecutionContext
from dagster import Output, MetadataValue
from dagster import AssetIn


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
                               videoCategory_trending_data: pl.DataFrame) -> Output[DataFrame]:
    """ 
        Clean 'videoCategory_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    context.log.info("Get Object Spark Session")
    spark_df: DataFrame = spark.createDataFrame(videoCategory_trending_data)
    context.log.info("Convert polars dataframe to pyspark dataframe")
    spark_df = spark_df.orderBy(spark_df["categoryId"])
    context.log.info("Sort pyspark dataframe by categoryId column")
    
    return Output(
        value=spark_df,
        metadata={
            "File Name": MetadataValue.text("silver_videoCategory_clean.pq"),
            "Number Columns": MetadataValue.int(len(spark_df.columns)),
            "Number Records": MetadataValue.int(spark_df.count())
        }
    )
    
    
@asset(
    ins={
        "linkVideos_trending_data": AssetIn(
            key_prefix=["bronze", "youtube"]
        )
    },
    deps=["bronze_youtube_trending_data"],
    name="silver_videoCategory_clean",
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
                            linkVideos_trending_data: pl.DataFrame) -> Output[DataFrame]:
    """ 
        Clean 'linkVideos_trending_data' and load to silver layer in MinIO
    """
    spark: SparkSession = context.resources.spark_io_manager.get_spark_session(context)
    linkVideos: DataFrame = spark.createDataFrame(linkVideos_trending_data)
    linkVideos.createOrReplaceTempView("linkVideos")
    pl_data: pl.DataFrame = context.resources.youtube_io_manager.downLoad_linkVideos(context)
    trending: DataFrame = spark.createDataFrame(pl_data)
    trending.createOrReplaceTempView("trending")
    query = """ 
                SELECT t.*
                FROM linkVideos lv RIGHT JOIN trending t
                    on lv.videoId = t.video_id
            """
    spark_df = spark.sql(query)
    
    return Output(
        value=spark_df,
        metadata={
            "File Name": MetadataValue.text("silver_linkVideos_clean.pq"),
            "Number Columns": MetadataValue.int(len(spark_df.columns)),
            "Number Records": MetadataValue.int(spark_df.count())
        }
    )