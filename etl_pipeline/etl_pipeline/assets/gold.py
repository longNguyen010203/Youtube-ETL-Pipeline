import polars as pl

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
    compute_kind="polars",
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
    compute_kind="polars"
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
    
    
