import os
from dagster import (
    sensor,
    asset,
    DailyPartitionsDefinition,
    SkipReason,
    get_dagster_logger,
    resource,
    build_resources,
    DefaultSensorStatus, AssetKey, RunRequest, ExperimentalWarning
)
from typing import Dict, Set
import json
from dagster import EventRecordsFilter, DagsterEventType

from datetime import datetime, timedelta
from dagster_pandas import DataFrame
import paramiko
from pathlib import Path
import pandas as pd
from SSH_DEMO.resources.credentials import the_credentials
from SSH_DEMO.resources.ssh import my_ssh_resource
from SSH_DEMO.ops.scd2_helpers import deduplicate_scd2

# TODO: later docker-compose the example
# TODO: before committing restructure like in the HN job for a nicer user experience
# TODO add some processing logic here (SCD2 via pyspark)

DATE_FORMAT = "%Y-%m-%d"
START_DATE = "2022-01-01"

daily_partitions_def = DailyPartitionsDefinition(start_date=START_DATE)

# path for the directory as served from the SFTP server
GLOBAL_PREFIX = "upload"

def _source_path_from_context(context):
    return (
        context.solid_def.output_defs[0].metadata["source_file_base_path"]
        + "/"
        + context.partition_key
        + "/"
        + context.solid_def.output_defs[0].metadata["source_file_name"]
    )

def read_csv_sftp_direct(sftp, remotepath: str, partition_key:str, *args, **kwargs) -> pd.DataFrame:
    """
    Read a file from a remote host using SFTP over SSH.
    Args:
        sftp: the already initialized paramikro SFTP session
        partition_key: the key of the processed partition
        *args: positional arguments to pass to pd.read_csv
        **kwargs: keyword arguments to pass to pd.read_csv
    Returns:
        a pandas DataFrame with data loaded from the remote host
    """
    # print(f'Reading: {remotepath}')
    remote_file = sftp.open(remotepath)
    dataframe = pd.read_csv(remote_file, *args, **kwargs)
    # print(extracted_date)
    dataframe['event_dt'] = partition_key
    now_ts = pd.Timestamp.now()  
    dataframe['load_ts'] = now_ts
    remote_file.close()
    sftp.close()
    #print(dataframe)
    return dataframe

@asset(
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "foo.csv"},
    required_resource_keys={"credentials", "ssh"},
    #io_manager_key="parquet_io_manager"
)
def foo_asset(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Processing file '{path}'")

    ssh = context.resources.ssh
    sftp = ssh.open_sftp()
    df = read_csv_sftp_direct(sftp, path, context.partition_key)
    print(df)
    return df

@asset(
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "bar.csv"},
    required_resource_keys={"credentials", "ssh"},
    #io_manager_key="parquet_io_manager"
)
def bar_asset(context):
    return _shared_helper(context)


@asset(
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "baz.csv"},
    required_resource_keys={"credentials", "ssh"},
    #io_manager_key="parquet_io_manager"
)
def baz_asset(context):
    return _shared_helper(context)


def _shared_helper(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Shared processing file '{path}'")

    ssh = context.resources.ssh
    sftp = ssh.open_sftp()
    df = read_csv_sftp_direct(sftp, path, context.partition_key)
    print(df)
    return df


#############
# TODO: add in 3x assets which are compacted (using SCD2 and pyspark). These are not partitioned 
# and should be triggered from some sensor on any input asset completion
#############

@asset(
    partitions_def=daily_partitions_def,
    required_resource_keys={"pyspark"},
)
#def baz_scd2_asset(context, baz_asset:DataFrame):
# TODO: how to 1) schedule after baz_asset partition is done 2) how to not feed the data inline here - but rather the reference to sparks dataframe for the full (all partitions encompassing asset) one 3) how to maintain dagit showing the lineage between the baz_asset and baz_scd2_asset?
# TODO what is the right IO manager here? spark parquet one? how can I make the existing one more generic?
def baz_scd2_asset(context, baz_asset:DataFrame):
    return _shared_helper_scd2(context)


def _shared_helper_scd2(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Shared processing file '{path}'")

    #dummy_s_scd2 = deduplicate_scd2(key=["key"], sort_changing_ignored=["ts"], time_column="ts", columns_to_ignore=[], df=dummy_s)
    #dummy_s_scd2.printSchema()
    #dummy_s_scd2.show()

    # TODO implement SCD2 deduplicatio (using spark)
    return 0

@asset#(io_manager_key="parquet_io_manager")
def combined_asset(context, foo_asset: DataFrame, bar_asset: DataFrame, baz_asset:DataFrame):
    get_dagster_logger().info(f"updating combined asset (globally for all partitions) once all 3 input assets for a specific partition_key (date) are done")

    # TODO: before feed the compacted assets
    # TODO add some processing logic here (via DBT) for joining it all up - or perhaps resort to pyspark as well
    # but DBT/duckdb would be much nicer (or spark via DBT, preferably duckdb though)
    df = pd.DataFrame({'foo':[1,2,3]})
    return df


def sftp_exists(sftp, path):
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False

def close(sftp, ssh):
    sftp.close()
    ssh.close()


from SSH_DEMO.resources import resource_defs_ssh

def make_date_file_sensor_for_asset(asset, asset_group):
    job_def = asset_group.build_job(name=asset.op.name + "_job", selection=[asset.op.name])

    @sensor(job=job_def, name=asset.op.name + "_sensor", default_status=DefaultSensorStatus.RUNNING)
    def date_file_sensor(context):
        with build_resources(resource_defs_ssh) as resources:
            ssh = resources.ssh
            sftp = ssh.open_sftp()


            last_processed_date = context.cursor
            if last_processed_date is None:
                next_date = START_DATE
            else:
                next_date = (
                    datetime.strptime(last_processed_date, DATE_FORMAT) + timedelta(days=1)
                ).strftime(DATE_FORMAT)

            path = asset.op.output_defs[0].metadata["source_file_base_path"] + "/" + next_date + "/" + asset.op.output_defs[0].metadata["source_file_name"]            
            if sftp_exists(sftp, path):
                context.update_cursor(next_date)
                close(sftp, ssh)
                return job_def.run_request_for_partition(next_date, run_key=path)
            else:
                close(sftp, ssh)
                return SkipReason(f"Did not find file {path}")

    return date_file_sensor


def make_multi_join_sensor_for_asset(asset, asset_group):
    job_def = asset_group.build_job(name=asset.op.name + "_job", selection=[asset.op.name])

    @sensor(job=job_def, name=asset.op.name + "_sensor", default_status=DefaultSensorStatus.RUNNING)
    def multi_asset_join_sensor(context):
        # https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#multi-asset-sensors
        # https://github.com/dagster-io/dagster/discussions/7306
        # perhaps useful: retries ==> https://github.com/dagster-io/dagster/discussions/7257
        # perhaps useful: pass a configuration value to multiple ops in a job ==> https://github.com/dagster-io/dagster/discussions/3213

        partition_keys = [partition.name for partition in daily_partitions_def.get_partitions()]
        last_partition_index = int(context.cursor) if context.cursor else -1
        curr_partition = partition_keys[last_partition_index + 1]

        asset_partition_materialized: Dict[AssetKey, bool] = {} # mapping of asset key to dictionary of materialization status by partition

        asset_keys = [AssetKey("foo_asset"), AssetKey("bar_asset"), AssetKey("baz_asset")]
        for asset_key in asset_keys:
            records = context.instance.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=asset_key,
                    asset_partitions=[curr_partition],
                )
            )
            asset_partition_materialized[asset_key] = True if len(records) else False # materialization record exists for partition

        if asset_partition_materialized[AssetKey("foo_asset")] and asset_partition_materialized[AssetKey("bar_asset")] and asset_partition_materialized[AssetKey("baz_asset")]:
            # yield job_def.run_request_for_partition(partition_key=curr_partition, run_key=None)
            #yield RunRequest(run_key=curr_partition)
            yield RunRequest(run_key=None)
            context.update_cursor(str(last_partition_index + 1))
    return multi_asset_join_sensor
