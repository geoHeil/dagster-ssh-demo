import warnings
import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from dagster import repository, AssetGroup

from SSH_DEMO.sensors.sftp_sensor_asset_real import foo_asset, combined_asset, bar_asset, baz_asset, make_date_file_sensor_for_asset, make_multi_join_sensor_for_asset

from SSH_DEMO.sensors.sftp_sensor_asset_real import baz_scd2_asset, make_single_sensor_for_asset, foo_scd2_asset, bar_scd2_asset
from SSH_DEMO.assets.sql_transformations import dbt_assets

from SSH_DEMO.resources import resource_defs, resource_defs_ingest

from dagster import in_process_executor

####################
from dagster import asset, AssetGroup, repository, DailyPartitionsDefinition
my_partitions_def = DailyPartitionsDefinition(start_date="2022-04-10")
@asset(partitions_def=my_partitions_def)
def a1():
    return 1


@asset
def a1_cleaned(a1):
    return 11


@asset(partitions_def=my_partitions_def)
def a2():
    return 2


@asset
def a2_cleaned(a2):
    return 22

@asset
def a1_a2_combined(a1_cleaned, a2_cleaned):
    return a1_cleaned + a2_cleaned


group_dummy = AssetGroup([a1, a2, a1_cleaned, a2_cleaned, a1_a2_combined])


#@repository
#def my_job():
#    return [group_dummy]
####################

asset_group_no_spark = AssetGroup([foo_asset, bar_asset, baz_asset],
    resource_defs=resource_defs_ingest
)

asset_group = AssetGroup([foo_asset, bar_asset, baz_asset, combined_asset,
    baz_scd2_asset, foo_scd2_asset, bar_scd2_asset,
] + dbt_assets,
    resource_defs=resource_defs,
    #executor_def=in_process_executor
)

asset_job = asset_group.build_job("all_assets")
asset_job_dummy = group_dummy.build_job("dummy_assets")

@repository
def SSH_DEMO():
    return [
        asset_job,
        asset_job_dummy,
        make_multi_join_sensor_for_asset(combined_asset, asset_group),
        make_date_file_sensor_for_asset(foo_asset, asset_group_no_spark),
        make_date_file_sensor_for_asset(bar_asset, asset_group_no_spark),
        make_date_file_sensor_for_asset(baz_asset, asset_group_no_spark),
        make_single_sensor_for_asset(baz_scd2_asset, baz_asset, asset_group),
        make_single_sensor_for_asset(foo_scd2_asset, foo_asset, asset_group),
        make_single_sensor_for_asset(bar_scd2_asset, bar_asset, asset_group),
    ]