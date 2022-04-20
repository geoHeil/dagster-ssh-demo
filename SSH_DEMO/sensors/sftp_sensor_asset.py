from datetime import datetime, timedelta

from dagster import (
    sensor, SkipReason,
    build_resources,
    DefaultSensorStatus
)

from SSH_DEMO.resources import resource_defs_ssh, START_DATE, DATE_FORMAT


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
        with build_resources(resource_defs_ssh) as resources:
            ssh = resources.ssh
            sftp = ssh.open_sftp()

            last_processed_date = context.cursor
            if last_processed_date is None:
                next_date = START_DATE
            else:
                next_date = (
                        datetime.strptime(last_processed_date, DATE_FORMAT) + timedelta(days=1)
                ).strftime(DATE_FORMAT)

            path = asset.op.output_defs[0].metadata["source_file_base_path"] + "/" + next_date + "/" + \
                   asset.op.output_defs[0].metadata["source_file_name"]
            if sftp_exists(sftp, path):
                context.update_cursor(next_date)
                close(sftp, ssh)
                return job_def.run_request_for_partition(next_date, run_key=path)
            else:
                close(sftp, ssh)
                return SkipReason(f"Did not find file {path}")

    return date_file_sensor
