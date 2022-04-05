from dagster import resource
import paramiko

@resource(config_schema={"remote_host": str, "remote_port": int}, required_resource_keys={"credentials"})
def my_ssh_resource(init_context):

    # TODO: does it perhaps make sense to not have a separate resource for credentials?
    credentials = init_context.resources.credentials
    user, password = credentials
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host = init_context.resource_config["remote_host"]
    port = init_context.resource_config["remote_port"]
    ssh.connect(host, port=port, username=user, password=password)
    return ssh
