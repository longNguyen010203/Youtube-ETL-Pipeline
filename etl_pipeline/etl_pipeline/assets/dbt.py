import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dbt import DagsterDbtTranslator

from typing import Mapping, Optional, Any



DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_tranform").resolve()
DBT_PROFILE_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_tranform").resolve()
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[str]:
        return "warehouse"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def Brazilian_ECommerce_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    