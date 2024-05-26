import os
from pathlib import Path
import polars as pl

from dagster_dbt import load_assets_from_dbt_project
from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext

from ..partitions import monthly_partitions

from dagster import (
    multi_asset,
    AssetIn,
    AssetOut,
    MetadataValue,
    AssetExecutionContext,
    Output
)


GROUP_NAME = "warehouse"

@multi_asset(
    ins={
        "gold_videoCategory": AssetIn(
            key_prefix=["gold", "youtube"],
        )
    },
    outs={
        "videoCategory": AssetOut(
            key_prefix=["gold"],
            io_manager_key="psql_io_manager",
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
    name="videoCategory",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres",
)
def videoCategory(context: AssetExecutionContext,
                       gold_videoCategory: pl.DataFrame
) -> Output[pl.DataFrame]:
    """ 
        Load videoCategory data from gold to PostgreSQL warehouse
    """
    pl_data: pl.DataFrame = gold_videoCategory
    context.log.info(f"Load videoCategory data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "table name": MetadataValue.text("videoCategory"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
        }
    )
    

@multi_asset(
    ins={
        "gold_linkVideos": AssetIn(
            key_prefix=["gold", "youtube"],
        )
    },
    outs={
        "linkVideos": AssetOut(
            key_prefix=["gold"],
            io_manager_key="psql_io_manager",
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
    name="linkVideos",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres"
)
def linkVideos(context: AssetExecutionContext,
                    gold_linkVideos: pl.DataFrame
) -> Output[pl.DataFrame]:
    """ 
        Load linkVideos data from gold to PostgreSQL warehouse
    """
    pl_data: pl.DataFrame = gold_linkVideos
    context.log.info(f"Load linkVideos data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "table name": MetadataValue.text("linkVideos"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
        }
    )
    
    
@multi_asset(
    ins={
        "gold_metric_trending": AssetIn(
            key_prefix=["gold", "youtube"]
        )
    },
    outs={
        "metricVideos": AssetOut(
            key_prefix=["gold"],
            io_manager_key="psql_io_manager",
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
        )
    },
    name="metricVideos",
    required_resource_keys={"psql_io_manager"},
    partitions_def=monthly_partitions,
    compute_kind="postgres"
)
def metricVideos(context: AssetExecutionContext,
                 gold_metric_trending: pl.DataFrame
) -> Output[pl.DataFrame]:
    """ 
        Load metricVideos data from gold to PostgreSQL warehouse
    """
    pl_data: pl.DataFrame = gold_metric_trending
    context.log.info(f"Load metricVideos data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "table name": MetadataValue.text("metricVideos"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
        }
    )
    
    
@multi_asset(
    ins={
        "gold_information_trending": AssetIn(
            key_prefix=["gold", "youtube"]
        )
    },
    outs={
        "informationVideos": AssetOut(
            key_prefix=["gold"],
            io_manager_key="psql_io_manager",
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
        )
    },
    name="informationVideos",
    required_resource_keys={"psql_io_manager"},
    partitions_def=monthly_partitions,
    compute_kind="postgres"
)
def informationVideos(context: AssetExecutionContext,
                 gold_information_trending: pl.DataFrame
) -> Output[pl.DataFrame]:
    """ 
        Load informationVideos data from gold to PostgreSQL warehouse
    """
    pl_data: pl.DataFrame = gold_information_trending
    context.log.info(f"Load informationVideos data Success with shape {pl_data.shape}")
    
    return Output(
        value=pl_data,
        metadata={
            "table name": MetadataValue.text("informationVideos"),
            "record count": MetadataValue.int(pl_data.shape[0]),
            "column count": MetadataValue.int(pl_data.shape[1]),
            "columns": pl_data.columns
        }
    )


DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_tranform").resolve()
DBT_PROFILE_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_tranform").resolve()
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def dbt_assets_bigquery(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
        
# dbt_assets = load_assets_from_dbt_project(
#     project_dir=os.fspath(DBT_PROJECT_DIR),
#     profiles_dir=os.fspath(DBT_PROFILE_DIR),
#     key_prefix=["dbt"]
# )