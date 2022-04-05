import os
from dagster import (
    sensor,
    asset,
    DailyPartitionsDefinition,
    SkipReason,
    get_dagster_logger,
    resource,
    build_resources,
    DefaultSensorStatus
)
from datetime import datetime, timedelta
import paramiko
from pathlib import Path
import pandas as pd

# TODO: before committing restructure like the HN job for a nicer user experience

DATE_FORMAT = "%Y-%m-%d"
START_DATE = "2022-01-01"

# path for the directory as served from the SFT server
GLOBAL_PREFIX = "upload/"


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
def combined_asset(context):
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
