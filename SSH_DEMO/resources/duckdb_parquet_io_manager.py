from operator import ge
import os

import duckdb
import pandas as pd

from dagster import Field, check, get_dagster_logger, io_manager
from dagster.seven.temp_dir import get_system_temp_directory
import pyspark

from .parquet_io_manager import PartitionedParquetIOManager


class DuckDBPartitionedParquetIOManager(PartitionedParquetIOManager):
    """Stores data in parquet files and creates duckdb views over those files."""

    def handle_output(self, context, obj):
        if obj is not None:  # if this is a dbt output, then the value will be None
            yield from super().handle_output(context, obj)
            con = self._connect_duckdb(context)

            path = self._get_path(context, get_base=True)
            if context.has_asset_partitions:
                # to_scan = os.path.join(os.path.dirname(path), "*.parquet")
                
                # directly run else case: all partitions shouldbe reigstered in duckDB
                to_scan = os.path.join(path, "*", "*.parquet")
            else:
                to_scan = path


            print('********')
            print(to_scan)
            get_dagster_logger().info(f'scanning: {to_scan}')
            # TODO account for hive-based partitioning!
            print('********')
            con.execute("create schema if not exists ssh_demo;")
            con.execute(
                f"create or replace view {self._table_path(context)} as "
                f"select * from read_parquet('{to_scan}');"
                #f"select * from parquet_scan('{to_scan}');"
            )

    def load_input(self, context):
        #check.invariant(not context.has_asset_partitions, "Can't load partitioned inputs")

        if context.dagster_type.typing_type == pd.DataFrame:
            con = self._connect_duckdb(context)
            return con.execute(f"SELECT * FROM {self._table_path(context)}").fetchdf()
        elif context.dagster_type.typing_type == pyspark.sql.DataFrame:
            # TODO figure out - or use native get path function from the parquet_io_manager
            #path = self._table_path(context)
            path = self._get_path(context, get_base=True)
            print('******************')
            print(path)
            get_dagster_logger().info(f'P: {path}')
            print('******************')
            return context.resources.pyspark.spark_session.read.parquet(path)

        check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _table_path(self, context):
        return f"ssh_demo.{context.asset_key.path[-1]}"

    def _connect_duckdb(self, context):
        return duckdb.connect(database=context.resource_config["duckdb_path"], read_only=False)


@io_manager(
    config_schema={"base_path": Field(str, is_required=False), "duckdb_path": str},
    required_resource_keys={"pyspark"},
)
def duckdb_partitioned_parquet_io_manager(init_context):
    import pathlib
    out_path = pathlib.Path() / "warehouse_location"
    out_path.mkdir(parents=True, exist_ok=True)
    
    return DuckDBPartitionedParquetIOManager(
        base_path=init_context.resource_config.get("base_path", str(out_path.resolve()))
    )
