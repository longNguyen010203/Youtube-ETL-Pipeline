import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import bronze, gold, silver, warehouse, dbt
from .resources import mysql, minio, postgres, youtube, spark


all_assets = load_assets_from_modules(
    [bronze, silver, gold, warehouse, dbt])

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": postgres,
        "youtube_io_manager": youtube,
        "spark_io_manager": spark,
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt.DBT_PROJECT_DIR),
            profiles_dir=os.fspath(dbt.DBT_PROFILE_DIR)
        ),
    },
)
