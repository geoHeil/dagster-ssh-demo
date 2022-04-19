import warnings
import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from dagster import repository, AssetGroup

from SSH_DEMO.sensors.sftp_sensor_asset_real import foo_asset, combined_asset, bar_asset, baz_asset, make_date_file_sensor_for_asset, make_multi_join_sensor_for_asset

from SSH_DEMO.sensors.sftp_sensor_asset_real import baz_scd2_asset, make_single_sensor_for_asset, foo_scd2_asset, bar_scd2_asset

from SSH_DEMO.resources import resource_defs

from dagster import in_process_executor

asset_group = AssetGroup([foo_asset, bar_asset, baz_asset, combined_asset,
    baz_scd2_asset, foo_scd2_asset, bar_scd2_asset
],
    resource_defs=resource_defs,
    #executor_def=in_process_executor
)

@repository
def SSH_DEMO():
    return [
        asset_group,
        make_multi_join_sensor_for_asset(combined_asset, asset_group),
        make_date_file_sensor_for_asset(foo_asset, asset_group),
        make_date_file_sensor_for_asset(bar_asset, asset_group),
        make_date_file_sensor_for_asset(baz_asset, asset_group),
        make_single_sensor_for_asset(baz_scd2_asset, baz_asset, asset_group),
        make_single_sensor_for_asset(foo_scd2_asset, foo_asset, asset_group),
        make_single_sensor_for_asset(bar_scd2_asset, bar_asset, asset_group),
    ]