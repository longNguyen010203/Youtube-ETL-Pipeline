import polars as pl

from dagster import asset, AssetExecutionContext
from dagster import Output, MetadataValue
from dagster import AssetIn

from ..partitions import monthly_partitions


GROUP_NAME = "bronze"

@asset(
    name="bronze_youtube_trending_data",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    partitions_def=monthly_partitions,
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_youtube_trending_data(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'bronze_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM youtube_trending_data """
    
    try:
        partition_date_str = context.asset_partition_key_for_output()
        partition_by = "publishedAt"
        
        query += f""" 
            WHERE SUBSTRING({partition_by}, 1, 4) = '{partition_date_str[:4]}' AND 
                  SUBSTRING({partition_by}, 6, 2) = '{partition_date_str[5:7]}'; """
                        
        context.log.info(f"Partition by {partition_by} = {partition_date_str[:7]}")
        context.log.info(f"SQL query: {query}")
    except Exception as e:
        context.log.info(f"olist_orders_dataset has no partition key!")
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'bronze_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "table": MetadataValue.text("youtube_trending_data"),
            "Number Columns": MetadataValue.int(pl_data.shape[1]),
            "Number Records": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    deps=[bronze_youtube_trending_data],
    name="videoCategory_trending_data",
    required_resource_keys={"youtube_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="Youtube API",
    group_name=GROUP_NAME
)
def videoCategory_trending_data(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Download Video Category from Youtube API by VideoId 
    """
    pl_data: pl.DataFrame = context \
            .resources \
            .youtube_io_manager \
            .downLoad_videoCategories(
                context
        )
    context.log.info("Download video category from youtube api success")
    
    return Output(
        value=pl_data,
        metadata={
            "File Name": MetadataValue.text("videoCategory_trending_data.pq"),
            "Number Columns": MetadataValue.int(pl_data.shape[1]),
            "Number Records": MetadataValue.int(pl_data.shape[0]),
        }
    )
    
    
@asset(
    deps=[bronze_youtube_trending_data],
    name="linkVideos_trending_data",
    required_resource_keys={"youtube_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="Youtube API",
    group_name=GROUP_NAME
)
def linkVideos_trending_data(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Download Video Category from Youtube API by VideoId 
    """
    pl_data: pl.DataFrame = context \
            .resources \
            .youtube_io_manager \
            .downLoad_linkVideos(
                context
        )
    context.log.info("Download link video from youtube api success")
    
    return Output(
        value=pl_data,
        metadata={
            "File Name": MetadataValue.text("linkVideos_trending_data.pq"),
            "Number Columns": MetadataValue.int(pl_data.shape[1]),
            "Number Records": MetadataValue.int(pl_data.shape[0]),
        }
    )
    
    
