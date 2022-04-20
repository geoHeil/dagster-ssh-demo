import pandas as pd
from dagster import asset, get_dagster_logger

from SSH_DEMO.resources import daily_partitions_def

# path for the directory as served from the SFTP server
GLOBAL_PREFIX = "upload"
DB_ZONE = "landing"


def _source_path_from_context(context):
    return (
            context.solid_def.output_defs[0].metadata["source_file_base_path"]
            + "/"
            + context.partition_key
            + "/"
            + context.solid_def.output_defs[0].metadata["source_file_name"]
    )


def read_csv_sftp_direct(sftp, remotepath: str, partition_key: str, *args, **kwargs) -> pd.DataFrame:
    """
    Read a file from a remote host using SFTP over SSH.
    Args:
        sftp: the already initialized paramikro SFTP session
        remotepath: the file path on the remote to read
        partition_key: the key of the processed partition
        *args: positional arguments to pass to pd.read_csv
        **kwargs: keyword arguments to pass to pd.read_csv
    Returns:
        a pandas DataFrame with data loaded from the remote host
    """
    remote_file = sftp.open(remotepath)
    dataframe = pd.read_csv(remote_file, *args, **kwargs)
    dataframe['event_dt'] = partition_key
    now_ts = pd.Timestamp.now()
    dataframe['load_ts'] = now_ts
    remote_file.close()
    sftp.close()
    return dataframe


@asset(
    compute_kind="python",
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "foo.csv", "db_zone": DB_ZONE},
    required_resource_keys={"credentials", "ssh"},
    # io_manager_key="parquet_io_manager"
)
def foo_asset(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Processing file '{path}'")

    ssh = context.resources.ssh
    sftp = ssh.open_sftp()
    df = read_csv_sftp_direct(sftp, path, context.partition_key)
    return df


@asset(
    compute_kind="python",
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "bar.csv", "db_zone": DB_ZONE},
    required_resource_keys={"credentials", "ssh"},
    # io_manager_key="parquet_io_manager"
)
def bar_asset(context):
    return _shared_helper(context)


@asset(
    compute_kind="python",
    partitions_def=daily_partitions_def,
    metadata={"source_file_base_path": GLOBAL_PREFIX, "source_file_name": "baz.csv", "db_zone": DB_ZONE},
    required_resource_keys={"credentials", "ssh"},
    # io_manager_key="parquet_io_manager"
)
def baz_asset(context):
    return _shared_helper(context)


def _shared_helper(context):
    path = _source_path_from_context(context)
    get_dagster_logger().info(f"Shared processing file '{path}'")

    ssh = context.resources.ssh
    sftp = ssh.open_sftp()
    df = read_csv_sftp_direct(sftp, path, context.partition_key)
    return df
