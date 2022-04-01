from dagster import op, job, DefaultSensorStatus, sensor, RunRequest, build_resources
import os
import re
import stat
import paramiko
from pathlib import Path


@op(config_schema={"filename": str})
def process_file_dummy_provided_resource(context):
    filename = context.op_config["filename"]
    context.log.info(filename)

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
                     "ops": {"process_file_dummy_provided_resource": {"config": {"filename": f}}}
                 },
             ))
    return file_list

@job
def log_file_job_remote_dummy_provided_resource():
    process_file_dummy_provided_resource()

resource_defs = {
    "ssh":{
        "config":{
            "remote_host": "localhost",
            "remote_port": 2222,
            'username': 'foo',
            'password': 'bar'
        }
    }
}

from dagster_ssh.resources import SSHResource
from dagster_ssh.resources import ssh_resource as sshresource

@sensor(job=log_file_job_remote_dummy_provided_resource, default_status=DefaultSensorStatus.RUNNING)
def my_directory_sensor_SFTP_provided_resource():
    with build_resources(
        { "ssh": sshresource}, resource_config=resource_defs
    ) as resources:

        ssh = resources.ssh

        # :rtype: paramiko.client.SSHClient
        # this internally is calling paramiko.client.SSHClient.connect (but failing)
        # before I manually did the same thing but succeeded. Where is the error? I cannot spot it right now.
        ssh.get_connection()
        sftp = ssh.open_sftp()

        # Actucal call of paramiko_glob.
        yield from paramiko_glob('upload/', '.*\.csv', sftp)

        sftp.close()
        ssh.close()
