import polars as pl
from ..partitions import monthly_partitions

from dagster import (
    asset,
    Output,
    AssetIn,
    AssetOut,
    multi_asset,
    MetadataValue,
    AssetExecutionContext
)


GROUP_NAME = "bronze"

@asset(
    name="bronze_CA_youtube_trending",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_CA_youtube_trending(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'CA_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM CA_youtube_trending_data; """
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'CA_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("CA_youtube_trending.pq"),
            "number columns": MetadataValue.int(pl_data.shape[1]),
            "number records": MetadataValue.int(pl_data.shape[0])
        }
    )
        
    
@asset(
    name="bronze_DE_youtube_trending",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_DE_youtube_trending(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'DE_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM DE_youtube_trending_data; """
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'DE_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("DE_youtube_trending.pq"),
            "number columns": MetadataValue.int(pl_data.shape[1]),
            "number records": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="bronze_IN_youtube_trending",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_IN_youtube_trending(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'IN_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM IN_youtube_trending_data; """
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'IN_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("IN_youtube_trending.pq"),
            "number columns": MetadataValue.int(pl_data.shape[1]),
            "number records": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="bronze_JP_youtube_trending",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_JP_youtube_trending(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'JP_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM JP_youtube_trending_data; """
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'JP_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("JP_youtube_trending.pq"),
            "number columns": MetadataValue.int(pl_data.shape[1]),
            "number records": MetadataValue.int(pl_data.shape[0])
        }
    )


@asset(
    name="bronze_RU_youtube_trending",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "youtube"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_RU_youtube_trending(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load table 'RU_youtube_trending_data'
        from MySQL database as polars DataFrame and save to MinIO
    """
    query = """ SELECT * FROM RU_youtube_trending_data; """
    
    pl_data: pl.DataFrame = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'RU_youtube_trending_data' from MySQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "file name": MetadataValue.text("RU_youtube_trending.pq"),
            "number columns": MetadataValue.int(pl_data.shape[1]),
            "number records": MetadataValue.int(pl_data.shape[0])
        }
    )


@multi_asset(
    ins={
        "bronze_CA_youtube_trending": AssetIn(key_prefix=["bronze", "youtube"]),
        "bronze_DE_youtube_trending": AssetIn(key_prefix=["bronze", "youtube"]),
        "bronze_IN_youtube_trending": AssetIn(key_prefix=["bronze", "youtube"])
    },
    outs={
        "silver_youtube_trending_01": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "youtube"],
            group_name="silver" #GROUP_NAME
        ),
        "bronze_linkVideos_trending": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "youtube"],
            group_name="silver" #GROUP_NAME
        )
    },
    name="bronze_linkVideos_trending",
    required_resource_keys={"youtube_io_manager"},
    compute_kind="Youtube API"
)
def bronze_linkVideos_trending(context: AssetExecutionContext,
                                bronze_CA_youtube_trending: pl.DataFrame,
                                bronze_DE_youtube_trending: pl.DataFrame,
                                bronze_IN_youtube_trending: pl.DataFrame):
    """
        Download Link Video from Youtube API by VideoId 
    """
    data = pl.concat(
        [
            bronze_CA_youtube_trending,
            bronze_DE_youtube_trending,
            bronze_IN_youtube_trending
        ]
    )
    # data = data.filter((pl.col("publishedAt") != "") | (pl.col("publishedAt") != "35"))
    # 2020-08-11T16:34:06Z
    data = data.with_columns(pl.col('publishedAt').apply(lambda e: e.replace('T', ' ').replace('Z', '')))
    data = data.with_columns(pl.col("publishedAt").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"))
    
    pl_data: pl.DataFrame = context \
            .resources \
            .youtube_io_manager \
            .downLoad_linkVideos(
                context, data
        )
    context.log.info("Download links video from youtube api success")
    
    return Output(
        value=data,
        output_name="silver_youtube_trending_01",
        metadata={
            "File Name": MetadataValue.text("youtube_trending_01.pq"),
            "Number Columns": MetadataValue.int(data.shape[1]),
            "Number Records": MetadataValue.int(data.shape[0])
        }
    ), Output(
        value=pl_data,
        output_name="bronze_linkVideos_trending",
        metadata={
            "File Name": MetadataValue.text("linkVideos_trending.pq"),
            "Number Columns": MetadataValue.int(pl_data.shape[1]),
            "Number Records": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@multi_asset(
    ins={
        "bronze_JP_youtube_trending": AssetIn(key_prefix=["bronze", "youtube"]),
        "bronze_RU_youtube_trending": AssetIn(key_prefix=["bronze", "youtube"])
    },
    outs={
        "silver_youtube_trending_02": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "youtube"],
            group_name="silver" #GROUP_NAME
        ),
        "bronze_videoCategory_trending": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["bronze", "youtube"],
            group_name="silver" #GROUP_NAME
        )
    },
    name="bronze_videoCategory_trending",
    required_resource_keys={"youtube_io_manager"},
    compute_kind="Youtube API",
)
def bronze_videoCategory_trending(context: AssetExecutionContext,
                                  bronze_JP_youtube_trending: pl.DataFrame,
                                  bronze_RU_youtube_trending: pl.DataFrame
    ):
    """
        Download Video Category from Youtube API by categoryId 
    """
    data = pl.concat(
        [
            bronze_JP_youtube_trending,
            bronze_RU_youtube_trending
        ]
    )
    # 2020-08-11T16:34:06Z
    data = data.with_columns(pl.col('publishedAt').apply(lambda e: e.replace('T', ' ').replace('Z', '')))
    data = data.with_columns(pl.col("publishedAt").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"))
    
    pl_data: pl.DataFrame = context \
            .resources \
            .youtube_io_manager \
            .downLoad_videoCategories(
                context, data
        )
    context.log.info("Download video category from youtube api success")
    
    return Output(
        value=data,
        output_name="silver_youtube_trending_02",
        metadata={
            "File Name": MetadataValue.text("youtube_trending_02.pq"),
            "Number Columns": MetadataValue.int(data.shape[1]),
            "Number Records": MetadataValue.int(data.shape[0]),
        }
    ), Output(
        value=pl_data,
        output_name="bronze_videoCategory_trending",
        metadata={
            "File Name": MetadataValue.text("videoCategory_trending.pq"),
            "Number Columns": MetadataValue.int(pl_data.shape[1]),
            "Number Records": MetadataValue.int(pl_data.shape[0]),
        }
    )