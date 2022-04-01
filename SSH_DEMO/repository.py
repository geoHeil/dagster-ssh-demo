from dagster import repository

from SSH_DEMO.sensors.sftp_sensor_dummy import my_directory_sensor_SFTP, log_file_job_remote_dummy
from SSH_DEMO.sensors.sftp_sensor_dummy_provided_resource import my_directory_sensor_SFTP_provided_resource, log_file_job_remote_dummy_provided_resource


@repository
def SSH_DEMO():
    """
    The repository definition for this SSH_DEMO Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [log_file_job_remote_dummy,
    
    # disabled as failing
    #log_file_job_remote_dummy_provided_resource
    ]
    schedules = []
    sensors = [my_directory_sensor_SFTP,
    
    # disabled as failing
    #my_directory_sensor_SFTP_provided_resource
    ]

    return jobs + schedules + sensors
