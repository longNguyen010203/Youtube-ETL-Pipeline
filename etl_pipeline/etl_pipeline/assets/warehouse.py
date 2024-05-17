import polars as pl

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
            key_prefix=["warehouse", "gold"],
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
        Compute and Load videoCategory data from silver to gold layer in MinIO
    """
    pl_data: pl.DataFrame = gold_videoCategory
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
        "gold_linkVideos": AssetIn(
            key_prefix=["gold", "youtube"],
        )
    },
    outs={
        "linkVideos": AssetOut(
            key_prefix=["warehouse", "gold"],
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
        Compute and Load linkVideos data from silver to gold layer in MinIO
    """
    pl_data: pl.DataFrame = gold_linkVideos
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