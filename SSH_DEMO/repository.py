import warnings

import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from dagster import define_asset_job, repository, with_resources

from SSH_DEMO.assets.combined_asset import combined_asset
from SSH_DEMO.assets.ingest_assets import bar_asset, baz_asset, foo_asset
from SSH_DEMO.assets.scd2_compressed_assets import (bar_scd2_asset,
                                                    baz_scd2_asset,
                                                    foo_scd2_asset)
from SSH_DEMO.assets.sql_transformations import dbt_assets
from SSH_DEMO.resources import resource_defs, resource_defs_ingest
from SSH_DEMO.sensors.sftp_sensor_asset import make_date_file_sensor_for_asset
from SSH_DEMO.sensors.transitive_asset_sensors import (
    make_multi_join_sensor_for_asset, make_single_sensor_for_asset)

assets_no_spark = with_resources([foo_asset, bar_asset, baz_asset],
                                  resource_defs=resource_defs_ingest
                                  )

assets = with_resources([foo_asset, bar_asset, baz_asset, combined_asset,
                          baz_scd2_asset, foo_scd2_asset, bar_scd2_asset,
                          ] + dbt_assets,
                         resource_defs=resource_defs)
                         # executor_def=in_process_executor

# asset_group_ddb = AssetGroup(dbt_assets,
#    resource_defs=resource_defs_dbt
# )

asset_job = define_asset_job("all_assets")
# asset_group_ddb_job = asset_group_ddb.build_job("dbt_assets_without_spark_resource")

@repository
def SSH_DEMO():
    return [
        *assets,
        asset_job,
        make_date_file_sensor_for_asset(foo_asset),
        make_date_file_sensor_for_asset(bar_asset),
        make_date_file_sensor_for_asset(baz_asset),
        make_single_sensor_for_asset(foo_scd2_asset, foo_asset),
        make_single_sensor_for_asset(bar_scd2_asset, bar_asset),
        make_single_sensor_for_asset(baz_scd2_asset, baz_asset),
        make_multi_join_sensor_for_asset(combined_asset),
    ]
