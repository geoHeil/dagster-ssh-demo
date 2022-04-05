from dagster import repository, AssetGroup

from SSH_DEMO.sensors.sftp_sensor_asset_real import foo_asset, combined_asset, bar_asset, baz_asset, make_date_file_sensor_for_asset, make_multi_join_sensor_for_asset#, resource_defs
from SSH_DEMO.resources.parquet_io_manager import local_partitioned_parquet_io_manager
from SSH_DEMO.resources.duckdb_parquet_io_manager import duckdb_partitioned_parquet_io_manager

from dagster.utils import file_relative_path
DBT_PROJECT_DIR = file_relative_path(__file__, "../ssh_demo_dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"
import os
from dagster_dbt import dbt_cli_resource
from dagster_pyspark import pyspark_resource

#duck_db_path = file_relative_path(__file__, "duckdb.local.duckdb")
dbt_local_resource = dbt_cli_resource.configured(
    # "vars": {"duckdb_path": duck_db_path}
    {"profiles_dir": DBT_PROFILES_DIR, "project_dir": DBT_PROJECT_DIR, "target": "local"}
)

configured_pyspark = pyspark_resource.configured(
    {
        "spark_conf": {
            "spark.driver.memory": "4G"
        }
    }
)

from SSH_DEMO.resources.credentials import the_credentials
from SSH_DEMO.resources.ssh import my_ssh_resource

resource_defs = {
    "credentials": the_credentials.configured({
                'username': 'foo',
                'password': 'bar'
        }),
    "ssh": my_ssh_resource.configured({
            "remote_host": "localhost",
            "remote_port": 2222
        }),
    "parquet_io_manager": local_partitioned_parquet_io_manager,
    "warehouse_io_manager": duckdb_partitioned_parquet_io_manager.configured(
        {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "ssh_demo.duckdb")}
        ),
    "pyspark": configured_pyspark,
    "dbt": dbt_local_resource,
}

asset_group = AssetGroup([foo_asset, bar_asset, baz_asset, combined_asset],
    resource_defs=resource_defs
)

@repository
def SSH_DEMO():
    return [
        asset_group,
        make_multi_join_sensor_for_asset(combined_asset, asset_group),
        make_date_file_sensor_for_asset(foo_asset, asset_group),
        make_date_file_sensor_for_asset(bar_asset, asset_group),
        make_date_file_sensor_for_asset(baz_asset, asset_group),
    ]