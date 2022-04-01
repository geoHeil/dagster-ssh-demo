from dagster import op, job, DefaultSensorStatus, resource, sensor, RunRequest, build_resources, asset
import os
import re
import stat
import paramiko
from pathlib import Path
from dagster import AssetMaterialization, AssetKey, AssetGroup


@op(config_schema={"filename": str})
def process_file_asset_workaround(context):
    filename = context.op_config["filename"]
    context.log.info(filename)

    path = filename
    prefix  = 'upload/'
    # we get multiple CSV files (per date directory).
    # each CSV represents a table/separate asset
    # we want to materialize each file into a separate asset
    asset_kind = path[len(prefix):].split('/')[-1].split('.')[0]

    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'dummy_asset__{asset_kind}'), description="Persisted result to storage",
            metadata={
                "path": filename
            },
        )
    )

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
                     "ops": {"process_file_asset_workaround": {"config": {"filename": f}}}
                 },
             ))
    return file_list

@job
def log_file_job_remote_asset_workaround():
    process_file_asset_workaround()

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

@sensor(job=log_file_job_remote_asset_workaround, default_status=DefaultSensorStatus.RUNNING)
def my_directory_sensor_SFTP_asset_workaround():
    with build_resources(
        { "credentials": the_credentials, "ssh": my_ssh_resource}, resource_config=resource_defs
    ) as resources:
        ssh = resources.ssh
        sftp = ssh.open_sftp()
        yield from paramiko_glob('upload/', '.*\.csv', sftp)

        sftp.close()
        ssh.close()


# case 4: trigger some computation afterwards
# DISABLED!!!
# DagsterInvalidDefinitionError: Input asset '["dummy_asset__foo"]' for asset 'my_asset_computation' is not produced by any of the provided asset ops and is not one of the provided sources 

# from dagster import AssetKey, asset_sensor, get_dagster_logger
# @asset
# def my_asset_computation(dummy_asset__foo):
    # case 4: trigger some computation afterwards
    # TODO get active partition / input key
#     get_dagster_logger().info.info('xxxxx')
    # return [4,5,6]

# dummy_asset_ag = AssetGroup(
     # assets=[my_asset_computation],
  #    source_assets=[],
 #     resource_defs={}, # resource_defs why can`t I pass these resource defs/configurations?
# )
# asset_job_next_step = dummy_asset_ag.build_job("real_asset_dummy_computation_next_step", selection=["my_asset_computation"])

#@asset_sensor(asset_key=AssetKey("dummy_asset__foo"), job=asset_job_next_step)
#def my_asset_sensor(context, asset_event):
#    yield RunRequest(
#        run_key=context.cursor,
#        run_config={
#            "ops": {
#                "read_materialization": {
#                    "config": {
#                        "asset_key": asset_event.dagster_event.asset_key.path,
#                    }
#                }
#            }
#        },
#    )