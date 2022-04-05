from dagster import repository, AssetGroup

from SSH_DEMO.sensors.sftp_sensor_dummy import my_directory_sensor_SFTP, log_file_job_remote_dummy
from SSH_DEMO.sensors.sftp_sensor_dummy_provided_resource import my_directory_sensor_SFTP_provided_resource, log_file_job_remote_dummy_provided_resource
from SSH_DEMO.sensors.sftp_sensor_asset_workaround import my_directory_sensor_SFTP_asset_workaround, log_file_job_remote_asset_workaround#, asset_job_next_step, my_asset_sensor
#from SSH_DEMO.sensors.sftp_sensor_asset_real import my_directory_sensor_SFTP_asset_real, asset_job
from SSH_DEMO.sensors.sftp_sensor_asset_real import foo_asset, combined_asset, bar_asset, baz_asset, make_date_file_sensor_for_asset

asset_group = AssetGroup([foo_asset, bar_asset, baz_asset, combined_asset])

@repository
def SSH_DEMO():
    return [
        asset_group,
        asset_group.build_job(name=combined_asset.op.name + "_job", selection=[combined_asset.op.name]),
        make_date_file_sensor_for_asset(foo_asset, asset_group),
        make_date_file_sensor_for_asset(bar_asset, asset_group),
        make_date_file_sensor_for_asset(baz_asset, asset_group),
    ]

# @repository
# def SSH_DEMO():
#     """
#     The repository definition for this SSH_DEMO Dagster repository.

#     For hints on building your Dagster repository, see our documentation overview on Repositories:
#     https://docs.dagster.io/overview/repositories-workspaces/repositories
#     """
#     jobs = [
#         # case 1
#         #log_file_job_remote_dummy,
    
#     # case 2
#     # disabled as failing
#     #log_file_job_remote_dummy_provided_resource

#     #case 3 workaround A
#     #log_file_job_remote_asset_workaround,

#     #case 3 real asset B
#     asset_job#,

#     #case 4
#     #asset_job_next_step
#     ]
#     schedules = []
#     sensors = [
#         # case 1
#         #my_directory_sensor_SFTP,
    
#     # case 2
#     # disabled as failing
#     #my_directory_sensor_SFTP_provided_resource

#     # case 3 workaround A
#     #my_directory_sensor_SFTP_asset_workaround,

#     # case 3 real asset B
#     my_directory_sensor_SFTP_asset_real#,

#     # case 4
#     #my_asset_sensor
#     ]

#     return jobs + schedules + sensors
