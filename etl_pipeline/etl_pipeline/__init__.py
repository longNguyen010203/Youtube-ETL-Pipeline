from dagster import Definitions, load_assets_from_modules

from .assets import bronze, gold, silver, warehouse
from .resources import mysql, minio, postgres, youtube, spark


all_assets = load_assets_from_modules(
    [bronze, silver, gold, warehouse])

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": postgres,
        "youtube_io_manager": youtube,
        "spark_io_manager": spark
    },
)
