import os
from typing import Union

import pandas
import pandas as pd
import pyspark

from dagster import Field, IOManager, MetadataEntry, OutputContext, check, io_manager
#from dagster.seven.temp_dir import get_system_temp_directory


class PartitionedParquetIOManager(IOManager):
    """
    This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    def __init__(self, base_path):
        self._base_path = base_path

    def handle_output(
        self, context: OutputContext, obj: Union[pandas.DataFrame, pyspark.sql.DataFrame]
    ):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False, compression='gzip')
        elif isinstance(obj, pyspark.sql.DataFrame):
            row_count = obj.count()
            obj.write.parquet(path=path, mode="overwrite", compression="gzip")
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")
        # TODO: include as ASSET Observation/Asset Materialization!
        yield MetadataEntry.int(value=row_count, label="row_count")
        yield MetadataEntry.path(path=path, label="path")

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y-%m-%d"
            dt_formatted = start.strftime(dt_format)
            yield MetadataEntry.text(dt_formatted, label="partition")

    def load_input(self, context) -> Union[pyspark.sql.DataFrame, str]:
        path = self._get_path(context.upstream_output)
        if context.dagster_type.typing_type == pyspark.sql.DataFrame:
            # return pyspark dataframe
            return context.resources.pyspark.spark_session.read.parquet(path)
        elif context.dagster_type.typing_type == pandas.DataFrame:
            return pd.read_parquet(path)

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: OutputContext, get_base=False):
        key = context.asset_key.path[-1]

        if get_base:
            return os.path.join(self._base_path, f"{key}")
        else:
            if context.has_asset_partitions:
                start, end = context.asset_partitions_time_window
                dt_format_long = "%Y%m%d%H%M%S"
                dt_format = "%Y%m%d"
                partition_str_long = start.strftime(dt_format_long) + "_" + end.strftime(dt_format_long)
                # is the same (for this dummy example)
                partition_str = start.strftime(dt_format)
                return os.path.join(self._base_path,  key, f'dt={partition_str}',f"{key}__{partition_str_long}.parquet")
            else:
                return os.path.join(self._base_path, f"{key}")


@io_manager(
    config_schema={"base_path": Field(str, is_required=False)},
    required_resource_keys={"pyspark"},
)
def local_partitioned_parquet_io_manager(init_context):
    import pathlib
    out_path = pathlib.Path() / "warehouse_location"
    out_path.mkdir(parents=True, exist_ok=True)
    
    return PartitionedParquetIOManager(
        base_path=init_context.resource_config.get("base_path", str(out_path.resolve()))
    )
