from dagster import Definitions, load_assets_from_modules

from .assets import bronze_layer, silver_layer, gold_layer, warehouse_layer


all_assets = load_assets_from_modules([bronze_layer, silver_layer, gold_layer, warehouse_layer])

defs = Definitions(
    assets=all_assets,
)
