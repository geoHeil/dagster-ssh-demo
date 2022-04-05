import os
from dagster import (
    sensor,
    asset,
    DailyPartitionsDefinition,
    SkipReason,
    get_dagster_logger,
    resource,
    build_resources,
    DefaultSensorStatus, AssetKey, RunRequest
)
import json
from dagster import EventRecordsFilter, DagsterEventType

from datetime import datetime, timedelta
from dagster_pandas import DataFrame
import paramiko
from pathlib import Path
import pandas as pd

# TODO: before committing restructure like the HN job for a nicer user experience

DATE_FORMAT = "%Y-%m-%d"
START_DATE = "2022-01-01"

# path for the directory as served from the SFT server
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
    return dataframe



@asset(
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "foo.csv"},
)
def foo_asset(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Processing file '{path}'")

    # TODO: how to get the resource here for the asset?
    #df = read_csv_sftp_direct(ssh, path, context.partition_key)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "bar.csv"},
)
def bar_asset(context):
    _shared_helper(context)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "baz.csv"},
)
def baz_asset(context):
    _shared_helper(context)


def _shared_helper(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Shared processing file '{path}'")


@asset
def combined_asset(context, foo_asset: DataFrame, bar_asset: DataFrame, baz_asset:DataFrame):
    get_dagster_logger().info(f"updating combined asset (globally for all partitions) once all 3 input assets for a specific partition_key (date) are done")


@resource(config_schema={"username": str, "password": str})
def the_credentials(init_context):
    user_resource = init_context.resource_config["username"]

    # it is better to read the password from the environment?
    pass_resource = init_context.resource_config["password"]
    return user_resource, pass_resource

@resource(config_schema={"remote_host": str, "remote_port": int}, required_resource_keys={"credentials"})
def my_ssh_resource(init_context):
    credentials = init_context.resources.credentials
    user, password = credentials
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host = init_context.resource_config["remote_host"]
    port = init_context.resource_config["remote_port"]
    ssh.connect(host, port=port, username=user, password=password)
    return ssh

resource_defs = {
    "credentials": { 
        'config': {
            'username': 'foo',
            'password': 'bar'
         },
    },
    "ssh":{
        "config":{
            "remote_host": "localhost",
            "remote_port": 2222,
        }
    }
}


def sftp_exists(sftp, path):
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False

def close(sftp, ssh):
    sftp.close()
    ssh.close()

def make_date_file_sensor_for_asset(asset, asset_group):
    job_def = asset_group.build_job(name=asset.op.name + "_job", selection=[asset.op.name])

    @sensor(job=job_def, name=asset.op.name + "_sensor", default_status=DefaultSensorStatus.RUNNING)
    def date_file_sensor(context):
        with build_resources(
            { "credentials": the_credentials, "ssh": my_ssh_resource}, resource_config=resource_defs
        ) as resources:
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
        cursor_dict = json.loads(context.cursor) if context.cursor else {}
        foo_cursor = cursor_dict.get("foo_asset")
        bar_cursor = cursor_dict.get("bar_asset")
        baz_cursor = cursor_dict.get("baz_asset")

        foo_event_records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=AssetKey("table_a"),
                after_cursor=foo_cursor,
            ),
            ascending=False,
            limit=1,
        )
        bar_event_records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=AssetKey("table_a"),
                after_cursor=bar_cursor,
            ),
            ascending=False,
            limit=1,
        )
        baz_event_records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=AssetKey("table_a"),
                after_cursor=baz_cursor,
            ),
            ascending=False,
            limit=1,
        )

        if not foo_event_records or not bar_event_records or not baz_event_records:
            return

        # make sure we only generate events if both table_foo and table_bar and table_baz have been materialized since
        # the last evaluation.
        yield RunRequest(run_key=None)

        # update the sensor cursor by combining the individual event cursors from the two separate
        # asset event streams
        context.update_cursor(
            json.dumps(
                {
                    "foo": foo_event_records[0].storage_id,
                    "bar": bar_event_records[0].storage_id,
                    "baz": baz_event_records[0].storage_id,
                }
            )
        )

    return multi_asset_join_sensor
