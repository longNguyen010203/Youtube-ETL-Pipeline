import os
import polars as pl
from datetime import datetime
from pyspark.sql import DataFrame

from dagster import (
    multi_asset,
    AssetIn,
    AssetOut,
    MetadataValue,
    AssetExecutionContext,
    Output
)

from ..partitions import monthly_partitions
from ..resources.spark_io_manager import create_spark_session


GROUP_NAME = "gold"

@multi_asset(
    ins={
        "silver_videoCategory_cleaned": AssetIn(
            key_prefix=["silver", "youtube"],
            input_manager_key="spark_io_manager"
        )
    },
    outs={
        "gold_videoCategory": AssetOut(
            key_prefix=["gold", "youtube"],
            io_manager_key="spark_io_manager",
            metadata={
                "primary_keys": [
                    "categoryId"
                ],
                "columns": [
                    "categoryId",
                    "categoryName"
                ]
            },
            group_name=GROUP_NAME
        )
    },
    name="gold_videoCategory",
    required_resource_keys={"spark_io_manager"},
    compute_kind="PySpark",
)
def gold_videoCategory(context: AssetExecutionContext,
                       silver_videoCategory_cleaned: pl.DataFrame
) -> Output[DataFrame]:
    """ 
        Compute and Load videoCategory data from silver to gold layer in MinIO
    """
    CONFIG = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    
    with create_spark_session(
        CONFIG, "gold_videoCategory-{}".format(datetime.today())
    ) as spark:
        spark_df: DataFrame = spark.createDataFrame(silver_videoCategory_cleaned.to_pandas())
    context.log.info(f"Load {context.asset_key.path[-1]} to gold layer success 🙂")
    
    return Output(
        value=spark_df,
        metadata={
            "file name": MetadataValue.text("videoCategory.pq"),
            "record count": MetadataValue.int(spark_df.count()),
            "column count": MetadataValue.int(len(spark_df.columns)),
            "columns": spark_df.columns
        }
    )
    
    
@multi_asset(
    ins={
        "silver_linkVideos_cleaned": AssetIn(
            key_prefix=["silver", "youtube"],
            input_manager_key="spark_io_manager"
        )
    },
    outs={
        "gold_linkVideos": AssetOut(
            key_prefix=["gold", "youtube"],
            io_manager_key="spark_io_manager",
            metadata={
                "primary_keys": [
                    "video_id"
                ],
                "columns": [
                    "video_id",
                    "link_video"
                ]
            },
            group_name=GROUP_NAME
        )
    },
    name="gold_linkVideos",
    required_resource_keys={"spark_io_manager"},
    compute_kind="PySpark"
)
def gold_linkVideos(context: AssetExecutionContext,
                    silver_linkVideos_cleaned: pl.DataFrame
) -> Output[DataFrame]:
    """ 
        Compute and Load linkVideos data from silver to gold layer in MinIO
    """
    
    CONFIG = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    
    with create_spark_session(
        CONFIG, "gold_linkVideos-{}".format(datetime.today())
    ) as spark:
        spark_df: DataFrame = spark.createDataFrame(silver_linkVideos_cleaned.to_pandas())
    context.log.info(f"Load {context.asset_key.path[-1]} to gold layer success 🙂")
    
    return Output(
        value=spark_df,
        metadata={
            "file name": MetadataValue.text("linkVideos.pq"),
            "record count": MetadataValue.int(spark_df.count()),
            "column count": MetadataValue.int(len(spark_df.columns)),
            "columns": spark_df.columns
        }
    )
    

@multi_asset(
    ins={
        "silver_trending_cleaned": AssetIn(
            key_prefix=["silver", "youtube"],
            input_manager_key="spark_io_manager"
        )
    },
    outs={
        "gold_metric_trending": AssetOut(
            key_prefix=["gold", "youtube"],
            io_manager_key="spark_io_manager",
            metadata={
                "primary_keys": [
                    "video_id"
                ],
                "columns": [
                    "video_id",
                    "publishedAt",
                    "trending_date",
                    "channelId",
                    "categoryId",
                    "view_count",
                    "likes",
                    "dislikes",
                    "comment_count"
                ]
            },
            group_name=GROUP_NAME
        ),
        "gold_information_trending": AssetOut(
            key_prefix=["gold", "youtube"],
            io_manager_key="spark_io_manager",
            metadata={
                "primary_keys": [
                    "video_id"
                ],
                "columns": [
                    "video_id",
                    "title",
                    "channelId",
                    "channelTitle",
                    "categoryId",
                    "tags",
                    "thumbnail_link",
                    "comments_disabled",
                    "ratings_disabled",
                ]
            },
            group_name=GROUP_NAME
        ),
    },
    name="gold_metric_trending",
    required_resource_keys={"spark_io_manager"},
    partitions_def=monthly_partitions,
    compute_kind="pyspark"
)
def gold_metric_trending(context: AssetExecutionContext,
                         silver_trending_cleaned: pl.DataFrame
):
    """ 
        Compute and Load trending data from silver to gold layer in MinIO
    """
    
    CONFIG = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    
    with create_spark_session(
        CONFIG, "gold_metric_trending-{}".format(datetime.today())
    ) as spark:
    
        metric: DataFrame = spark.createDataFrame(silver_trending_cleaned.select([
                        "video_id",
                        "publishedAt",
                        "trending_date",
                        "channelId",
                        "categoryId",
                        "view_count",
                        "likes",
                        "dislikes",
                        "comment_count"
                    ]))
        information: DataFrame = spark.createDataFrame(silver_trending_cleaned.select([
                        "video_id",
                        "title",
                        "channelId",
                        "channelTitle",
                        "categoryId",
                        "tags",
                        "thumbnail_link",
                        "comments_disabled",
                        "ratings_disabled",
                    ]))
    context.log.info(f"Load {context.asset_key.path[-1]} to gold layer success 🙂")
    
    return Output(
        value=metric,
        output_name="gold_metric_trending",
        metadata={
            "folder name": MetadataValue.text("metric_trending"),
            "record count": MetadataValue.int(metric.count()),
            "column count": MetadataValue.int(len(metric.columns)),
            "columns": metric.columns
        }
    ), Output(
        value=information,
        output_name="gold_information_trending",
        metadata={
            "folder name": MetadataValue.text("information_trending"),
            "record count": MetadataValue.int(information.count()),
            "column count": MetadataValue.int(len(information.columns)),
            "columns": information.columns
        }
    ),