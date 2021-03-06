import os

import duckdb
import pandas as pd
import pyspark
from dagster import Field, check, get_dagster_logger, io_manager

from SSH_DEMO.resources.locking_utils import FileLock
from .parquet_io_manager import PartitionedParquetIOManager


class DuckDBPartitionedParquetIOManager(PartitionedParquetIOManager):
    """Stores data in parquet files and creates duckdb views over those files."""

    def handle_output(self, context, obj):
        if obj is not None:  # if this is a dbt output, then the value will be None
            yield from super().handle_output(context, obj)
            con = self._connect_duckdb(context)

            path = self._get_path(context, get_base=True)
            get_dagster_logger().info(
                f'partitions: {context.has_asset_partitions}, type: {context.dagster_type.typing_type} ***')
            if context.has_asset_partitions:
                # to_scan = os.path.join(os.path.dirname(path), "*.parquet")

                # directly run else case: all partitions shouldbe reigstered in duckDB
                to_scan = os.path.join(path, "*", "*.parquet")
            else:
                # we fact two choices here: 1) it is a pandas dataframe (a single file is written)
                # 2) it is a spark dataframe and a whole directory is created
                # However, (3) if this is the last point of the IO manager the output might be any

                if context.dagster_type.typing_type == pd.DataFrame:
                    to_scan = path
                else:
                    # if context.dagster_type.typing_type == pyspark.sql.DataFrame:
                    # we get a whole folder back!
                    to_scan = os.path.join(path, "*.parquet")
                # else:
                #    to_scan = path
            schema = self.get_schema(context)

            get_dagster_logger().info(f'scanning: {to_scan} for schema: {schema}')
            con.execute(f"create schema if not exists {schema};")
            con.execute(
                f"create or replace view {self._table_path(context)} as "
                f"select * from read_parquet('{to_scan}');"
            )

    def load_input(self, context):
        # check.invariant(not context.has_asset_partitions, "Can't load partitioned inputs")

        if context.dagster_type.typing_type == pd.DataFrame:
            con = self._connect_duckdb(context)
            return con.execute(f"SELECT * FROM {self._table_path(context)}").fetchdf()
        elif context.dagster_type.typing_type == pyspark.sql.DataFrame:
            # TODO figure out - or use native get path function from the parquet_io_manager
            # path = self._table_path(context)
            path = self._get_path(context, get_base=True)
            # print('******************')
            # print(path)
            get_dagster_logger().info(f'Loading from input: {path}')
            # print('******************')
            return context.resources.pyspark.spark_session.read.parquet(path)

        check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _table_path(self, context):
        schema = self.get_schema(context)
        return f"{schema}.{context.asset_key.path[-1]}"

    def get_schema(self, context):
        metadata = context.metadata
        if 'db_zone' in metadata:
            schema = metadata['db_zone']
        else:
            schema = "ssh_demo"
        return schema
        # return "ssh_demo"

    def _connect_duckdb(self, context):
        ddb_path = context.resource_config["duckdb_path"]
        # https://stackoverflow.com/questions/489861/locking-a-file-in-python
        # https://dagster.slack.com/archives/C01U954MEER/p1649886081687719?thread_ts=1649863868.798109&cid=C01U954MEER
        # 
        with FileLock(ddb_path):
            # work with the file as it is now locked
            get_dagster_logger().info("Lock acquired.")
            return duckdb.connect(database=ddb_path, read_only=False)


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
