from dagster import repository

from SSH_DEMO.sensors.sftp_sensor_dummy import my_directory_sensor_SFTP, log_file_job_remote_dummy
from SSH_DEMO.sensors.sftp_sensor_dummy_provided_resource import my_directory_sensor_SFTP_provided_resource, log_file_job_remote_dummy_provided_resource
from SSH_DEMO.sensors.sftp_sensor_asset_workaround import my_directory_sensor_SFTP_asset_workaround, log_file_job_remote_asset_workaround


@repository
def SSH_DEMO():
    """
    The repository definition for this SSH_DEMO Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [
        # case 1
        log_file_job_remote_dummy,
    
    # case 2
    # disabled as failing
    #log_file_job_remote_dummy_provided_resource

    #case 3 workaround
    log_file_job_remote_asset_workaround
    ]
    schedules = []
    sensors = [
        # case 1
        my_directory_sensor_SFTP,
    
    # case 2
    # disabled as failing
    #my_directory_sensor_SFTP_provided_resource

    # case 3 workaround
    my_directory_sensor_SFTP_asset_workaround

    ]

    return jobs + schedules + sensors
