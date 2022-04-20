import warnings

import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from SSH_DEMO.assets.combined_asset import combined_asset
from SSH_DEMO.assets.ingest_assets import foo_asset, bar_asset, baz_asset
from SSH_DEMO.assets.scd2_compressed_assets import baz_scd2_asset, foo_scd2_asset, bar_scd2_asset
from SSH_DEMO.sensors.sftp_sensor_asset import make_date_file_sensor_for_asset
from SSH_DEMO.assets.sql_transformations import dbt_assets
from SSH_DEMO.sensors.transitive_asset_sensors import make_single_sensor_for_asset, make_multi_join_sensor_for_asset
from SSH_DEMO.resources import resource_defs, resource_defs_ingest

from dagster import repository, AssetGroup

asset_group_no_spark = AssetGroup([foo_asset, bar_asset, baz_asset],
                                  resource_defs=resource_defs_ingest
                                  )

asset_group = AssetGroup([foo_asset, bar_asset, baz_asset, combined_asset,
                          baz_scd2_asset, foo_scd2_asset, bar_scd2_asset,
                          ] + dbt_assets,
                         resource_defs=resource_defs,
                         # executor_def=in_process_executor
                         )

# asset_group_ddb = AssetGroup(dbt_assets,
#    resource_defs=resource_defs_dbt
# )

asset_job = asset_group.build_job("all_assets")
# asset_group_ddb_job = asset_group_ddb.build_job("dbt_assets_without_spark_resource")

@repository
def SSH_DEMO():
    return [
        asset_job,
        make_date_file_sensor_for_asset(foo_asset, asset_group_no_spark),
        make_date_file_sensor_for_asset(bar_asset, asset_group_no_spark),
        make_date_file_sensor_for_asset(baz_asset, asset_group_no_spark),
        make_single_sensor_for_asset(foo_scd2_asset, foo_asset, asset_group),
        make_single_sensor_for_asset(bar_scd2_asset, bar_asset, asset_group),
        make_single_sensor_for_asset(baz_scd2_asset, baz_asset, asset_group),
        make_multi_join_sensor_for_asset(combined_asset, asset_group),
    ]
