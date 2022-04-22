from dagster import DailyPartitionsDefinition
from dagster.utils import file_relative_path

START_DATE = "2022-01-01"
DATE_FORMAT = "%Y-%m-%d"
daily_partitions_def = DailyPartitionsDefinition(start_date=START_DATE)

DBT_PROJECT_DIR = file_relative_path(__file__, "../../ssh_demo_dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"
import os
from dagster_dbt import dbt_cli_resource
from dagster_pyspark import pyspark_resource

# duck_db_path = file_relative_path(__file__, "duckdb.local.duckdb")
dbt_local_resource = dbt_cli_resource.configured(
    # "vars": {"duckdb_path": duck_db_path}
    # , "target": "local"
    {"profiles_dir": DBT_PROFILES_DIR, "project_dir": DBT_PROJECT_DIR}
)

configured_pyspark = pyspark_resource.configured(
    {
        "spark_conf": {
            "spark.driver.memory": "8G",
            "spark.sql.session.timeZone": "UTC",
            "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewedJoin.enabled": "true",
            "spark.sql.cbo.enabled": "true",
            "spark.sql.cbo.joinReorder.enabled": "true",
            "spark.sql.cbo.starSchemaDetection": "true",
            "spark.sql.autoBroadcastJoinThreshold": "200MB",
            "spark.sql.statistics.histogram.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true"
        }
    }
)

from SSH_DEMO.resources.credentials import the_credentials
from SSH_DEMO.resources.ssh import my_ssh_resource
from SSH_DEMO.resources.dummy_pyspark import my_dummy_pyspark_resource
from SSH_DEMO.resources.parquet_io_manager import local_partitioned_parquet_io_manager
from SSH_DEMO.resources.duckdb_parquet_io_manager import duckdb_partitioned_parquet_io_manager

resource_defs_ssh = {
    "credentials": the_credentials.configured({"username": "foo", "password": "bar"}),
    # local execution
    #"ssh": my_ssh_resource.configured({"remote_host": "localhost", "remote_port": 2222}),
    # in docker it must be a different host
    "ssh": my_ssh_resource.configured({"remote_host": "sftp", "remote_port": 22}),
}

resource_defs_pyspark = {
    "pyspark": configured_pyspark,
}

resource_defs_ingest = {
    **resource_defs_ssh,
    "io_manager": duckdb_partitioned_parquet_io_manager.configured(
        {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "ssh_demo.duckdb")}
    ),
    "pyspark": my_dummy_pyspark_resource,
    "dbt": dbt_local_resource,
}

resource_defs_dbt = {
    "io_manager": duckdb_partitioned_parquet_io_manager.configured(
        {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "ssh_demo.duckdb")}
    ),
    "dbt": dbt_local_resource,
}

resource_defs = {
    **resource_defs_ssh,
    **resource_defs_pyspark,
    # "io_manager": local_partitioned_parquet_io_manager,
    # "parquet_io_manager": local_partitioned_parquet_io_manager,
    "io_manager": duckdb_partitioned_parquet_io_manager.configured(
        # "warehouse_io_manager": duckdb_partitioned_parquet_io_manager.configured(
        {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "ssh_demo.duckdb")}
    ),

    "dbt": dbt_local_resource,
}
