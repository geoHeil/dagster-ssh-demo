from dagster import repository

from SSH_DEMO.sensors.sftp_sensor_dummy import my_directory_sensor_SFTP, log_file_job_remote_dummy


@repository
def SSH_DEMO():
    """
    The repository definition for this SSH_DEMO Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [log_file_job_remote_dummy]
    schedules = []
    sensors = [my_directory_sensor_SFTP]

    return jobs + schedules + sensors
