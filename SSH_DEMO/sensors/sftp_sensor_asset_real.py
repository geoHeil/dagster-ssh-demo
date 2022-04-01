from dagster import op, job, DefaultSensorStatus, resource, sensor, RunRequest, build_resources
import os
import re
import stat
import paramiko
from pathlib import Path
from dagster import AssetMaterialization, AssetKey

# Example remote SFTP sensor
# spin up the docker-compose file
# based on: https://gist.github.com/lkluft/ddda28208f7658d93f8238ad88bd45f2
def paramiko_glob(path, pattern, sftp):
    """Search recursively for files matching a given pattern.
    
    Parameters:
        path (str): Path to directory on remote machine.
        pattern (str): Python re [0] pattern for filenames.
        sftp (SFTPClient): paramiko SFTPClient.
        
    [0] https://docs.python.org/2/library/re.html
        
    """
    p = re.compile(pattern)
    root = sftp.listdir(path)
    file_list = []
    
    # Loop over all entries in given path...
    for f in (os.path.join(path, entry) for entry in root):
        f_stat = sftp.stat(f)
        # ... if it is a directory call paramiko_glob recursively.
        if stat.S_ISDIR(f_stat.st_mode):
            file_list += paramiko_glob(f, pattern, sftp)
        # ... if it is a file, check the name pattern and append it to file_list.
        elif p.match(f):
            #file_list.append(f)
            file_list.append(RunRequest(
                 run_key=f,
                 run_config={
                     "ops": {
                         "my_asset": {
                             #"config": {"filename": f}
                             # It is very unclear to me how to propagate the details here over to the Asset
                             "config": {
                                 "assets": {
                                     "input_partitions" : {"filename": f}
                                 }
                             }
                         }
                     }
                     #"ops": {"filename": {"config": {"filename": f}}}#,
                     #"assets": {"filename": {"config": {"filename": f}}},
                     #"my_asset": {"filename": {"config": {"filename": f}}}
                 },
             ))
    return file_list

@resource(config_schema={"username": str, "password": str})
def the_credentials(init_context):
    user_resource = init_context.resource_config["username"]

    # TODO: perhaps it is better to read the password from the environment?
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

from dagster import AssetGroup, SourceAsset, AssetKey
from dagster import asset, get_dagster_logger

# this fails as well
#@asset(config_schema={"filename": str})
@asset
def my_asset(context):
    # TODO: where to get the remote file name as the input to the asset?
    # The task is to retrieve the CSV and store it via some IO manager
    # Multiple CSV file names (tables) are present in each folder (date)
    # for each - compute an output. Not sure if this might be called dynamic asset?
    # Perhaps also the setup of the graph needs to be different?

    #filename = context.asset_config["filename"]
    #filename = context.op_config["filename"]
    #context.log.info(filename)
    #get_dagster_logger().info.info(file_path)

    # TODO: generate one asset per filename (once the asset-based-approach works at all)
    return [1, 2, 3]

dummy_asset_ag = AssetGroup(
     assets=[my_asset],
     source_assets=[],
     resource_defs={}, # resource_defs why can`t I pass these resource defs/configurations?
)
asset_job = dummy_asset_ag.build_job("real_asset_dummy", selection=["my_asset"])
@sensor(job=asset_job,default_status=DefaultSensorStatus.RUNNING)
def my_directory_sensor_SFTP_asset_real():
    with build_resources(
        { "credentials": the_credentials, "ssh": my_ssh_resource}, resource_config=resource_defs
    ) as resources:
        ssh = resources.ssh
        sftp = ssh.open_sftp()
        yield from paramiko_glob('upload/', '.*\.csv', sftp)

        sftp.close()
        ssh.close()
