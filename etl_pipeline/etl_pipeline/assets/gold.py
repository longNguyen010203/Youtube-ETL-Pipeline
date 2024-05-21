import polars as pl
from ..partitions import monthly_partitions

from dagster import (
    multi_asset,
    AssetIn,
    AssetOut,
    MetadataValue,
    AssetExecutionContext,
    Output
)


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
) -> Output[pl.DataFrame]:
    """ 
        Compute and Load videoCategory data from silver to gold layer in MinIO
    """
    pl_data: pl.DataFrame = silver_videoCategory_cleaned
    context.log.info(f"Load videoCategory data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("videoCategory.pq"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
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
) -> Output[pl.DataFrame]:
    """ 
        Compute and Load linkVideos data from silver to gold layer in MinIO
    """
    pl_data: pl.DataFrame = silver_linkVideos_cleaned
    context.log.info(f"Load linkVideos data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("linkVideos.pq"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
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
                    # "country_code",
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
                    # "country_code",
                    "title",
                    "channelId",
                    "channelTitle",
                    "categoryId",
                    "tags",
                    "thumbnail_link",
                    "comments_disabled",
                    "ratings_disabled",
                    # "description"
                ]
            },
            group_name=GROUP_NAME
        ),
        # "gold_metric_stats_by_country": AssetOut(
        #     key_prefix=["gold", "youtube"],
        #     io_manager_key="spark_io_manager",
        #     metadata={
        #         "primary_keys": [
        #             "video_id"
        #         ],
        #         "columns": [
        #             "video_id",
        #             "country_code",
        #             "title",
        #             "channelId",
        #             "channelTitle",
        #             "categoryId",
        #             "tags",
        #             "thumbnail_link",
        #             "comments_disabled",
        #             "ratings_disabled",
        #             "description"
        #         ]
        #     },
        #     group_name=GROUP_NAME
        # )
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
    metric: pl.DataFrame = silver_trending_cleaned.select([
                    "video_id",
                    # "country_code",
                    "publishedAt",
                    "trending_date",
                    "channelId",
                    "categoryId",
                    "view_count",
                    "likes",
                    "dislikes",
                    "comment_count"
                ])
    information: pl.DataFrame = silver_trending_cleaned.select([
                    "video_id",
                    # "country_code",
                    "title",
                    "channelId",
                    "channelTitle",
                    "categoryId",
                    "tags",
                    "thumbnail_link",
                    "comments_disabled",
                    "ratings_disabled",
                    # "description"
                ])
    
    # metric_stats_by_country: pl.DataFrame = metric.group_by("country_code").agg(
    #     [
    #         pl.sum("view_count").alias("total_views"),
    #         pl.sum("likes").alias("total_likes"),
    #         pl.sum("dislikes").alias("total_dislikes"),
    #         pl.sum("comment_count").alias("total_comments")
    #     ]
    # )
    
    return Output(
        value=metric,
        output_name="gold_metric_trending",
        metadata={
            "folder name": MetadataValue.text("metric_trending"),
            "record count": MetadataValue.int(metric.shape[0]),
            "column count": MetadataValue.int(metric.shape[1]),
            "columns": metric.columns
        }
    ), Output(
        value=information,
        output_name="gold_information_trending",
        metadata={
            "folder name": MetadataValue.text("information_trending"),
            "record count": MetadataValue.int(information.shape[0]),
            "column count": MetadataValue.int(information.shape[1]),
            "columns": information.columns
        }
    ),
    # Output(
    #     value=metric_stats_by_country,
    #     output_name="gold_metric_stats_by_country",
    #     metadata={
    #         "folder name": MetadataValue.text("metric_stats_by_country"),
    #         "record count": MetadataValue.int(metric_stats_by_country.shape[0]),
    #         "column count": MetadataValue.int(metric_stats_by_country.shape[1]),
    #         "columns": metric_stats_by_country.columns
    #     }
    # )