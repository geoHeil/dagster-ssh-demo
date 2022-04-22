import warnings

import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from typing import Dict

from dagster import sensor, DefaultSensorStatus, AssetKey, EventRecordsFilter, DagsterEventType, RunRequest, \
    asset_sensor, asset, AssetGroup, get_dagster_logger

from SSH_DEMO.resources import daily_partitions_def


def make_multi_join_sensor_for_asset(asset, asset_group):
    """
    For the 3 input assets foo, bar, baz (and their respective daily partition)
    AND their derived <<xx>>_scd2 compacted representation
    all need to be completed before triggering the downstream (combined) asset.
    """
    job_def = asset_group.build_job(name=asset.op.name + "_job", selection=[asset.op.name])

    @sensor(job=job_def, name=asset.op.name + "_sensor", default_status=DefaultSensorStatus.RUNNING)
    def multi_asset_join_sensor(context):
        # https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#multi-asset-sensors
        # https://github.com/dagster-io/dagster/discussions/7306
        # perhaps useful: retries ==> https://github.com/dagster-io/dagster/discussions/7257
        # perhaps useful: pass a configuration value to multiple ops in a job ==> https://github.com/dagster-io/dagster/discussions/3213

        partition_keys = [partition.name for partition in daily_partitions_def.get_partitions()]
        last_partition_index = int(context.cursor) if context.cursor else -1
        curr_partition = partition_keys[last_partition_index + 1]

        ######################################
        # (omitted step 0):
        # emit the partition metadata from the upstream asset: done

        # https://dagster.slack.com/archives/C01U954MEER/p1650057373811599?thread_ts=1649909251.959589&cid=C01U954MEER
        # think that putting metadata on the AssetMaterializations with the latest partition that they include would be the right approach.  Currently, this is a little complex, but doable.
        # First step: in the sensor for a1_cleaned, put a tag on the run with the partition for a1:
        # yield RunRequest(tags={"latest_partition": ...}): done

        # Then, in your IOManager, grab that tag and add it as metadata that will show up on the materialization for a1_cleaned:
        # partition_tag_value = context.step_context._plan_data.pipeline_run.tags["latest_partition"]
        # context.add_output_metadata({"latest_partition": }) (done)

        # Then, in the sensor for combined, you can access the metadata on a1_cleaned with something like the following:
        # materialization = record.event_log_entry.dagster_event.event_specific_data.materialization
        # latest_partition = [me for me in materialization.metadata_entries if me.label == "latest_partition"][0].entry_data.text
        ######################################

        asset_partition_materialized: Dict[
            AssetKey, bool] = {}  # mapping of asset key to dictionary of materialization status by partition

        # asset_keys_partitioned = [AssetKey("foo_asset"), AssetKey("bar_asset"), AssetKey("baz_asset")]
        # asset_keys_non_partitioned = [#AssetKey("foo_scd2_asset"), AssetKey("bar_scd2_asset"),
        # AssetKey("baz_scd2_asset")]

        asset_keys = [(AssetKey("foo_asset"), True), (AssetKey("bar_asset"), True), (AssetKey("baz_asset"), True),
                      (AssetKey("foo_scd2_asset"), False), (AssetKey("bar_scd2_asset"), False),
                      (AssetKey("baz_scd2_asset"), False)
                      ]
        # TODO: is there any better way to get this to work? This seems really clumnsy?
        # https://dagster.slack.com/archives/C01U954MEER/p1650011335134819?thread_ts=1649909251.959589&cid=C01U954MEER
        # How to tie the two separate materialization events together? Perhaps with additional metadata?
        for asset_key, is_partitioned in asset_keys:
            if is_partitioned:
                records = context.instance.get_event_records(
                    EventRecordsFilter(
                        event_type=DagsterEventType.ASSET_MATERIALIZATION,
                        asset_key=asset_key,
                        asset_partitions=[curr_partition],
                    )
                )
                asset_partition_materialized[asset_key] = True if len(
                    records) else False  # materialization record exists for partition
            else:
                records = context.instance.get_event_records(
                    EventRecordsFilter(
                        event_type=DagsterEventType.ASSET_MATERIALIZATION,
                        asset_key=asset_key,
                        # this is an unpartitioned asset
                        # how can the previous and this materialization be stringed together?
                        # must some additional metadata be used somehow?
                        # asset_partitions=[curr_partition],
                    ),
                    # how can this perhaps become a filter above to filter on the graphql side?
                    ascending=False,
                    limit=1,
                )
                if len(records) > 0:
                    records = records[0]
                    materialization = records.event_log_entry.dagster_event.event_specific_data.materialization
                    latest_partition = \
                        [me for me in materialization.metadata_entries if me.label == "latest_partition"][
                            0].entry_data.text
                    get_dagster_logger().info(f'Curr_part {curr_partition}: latest: {latest_partition} for asset: {asset_key}')
                    asset_partition_materialized[
                        asset_key] = True if latest_partition == curr_partition else False  # materialization record exists for upstream partition for stateful SCD2 asset
                else:
                    asset_partition_materialized[asset_key] = False

        # TODO fix this and only trigger when also the intermediate cleaned ones are done!!!
        if (asset_partition_materialized[AssetKey("foo_asset")] and asset_partition_materialized[
            AssetKey("bar_asset")] and asset_partition_materialized[AssetKey("baz_asset")] and
            asset_partition_materialized[AssetKey("baz_scd2_asset")]) and asset_partition_materialized[
            AssetKey("foo_scd2_asset")] and asset_partition_materialized[AssetKey("bar_scd2_asset")]:
            # yield job_def.run_request_for_partition(partition_key=curr_partition, run_key=None)
            # yield RunRequest(run_key=curr_partition)
            yield RunRequest(run_key=None, tags={"latest_partition": curr_partition})
            context.update_cursor(str(last_partition_index + 1))

    return multi_asset_join_sensor


def make_single_sensor_for_asset(asset, triggering_asset: asset, asset_group: AssetGroup):
    job_def = asset_group.build_job(name=asset.op.name + "_job", selection=[asset.op.name])

    # TODO: is this assumption valid that asset_key is identical to op.name?
    # https://dagster.slack.com/archives/C01U954MEER/p1650011014029099
    # https://dagster.slack.com/archives/C01U954MEER/p1650037529532929?thread_ts=1650011014.029099&cid=C01U954MEER
    # I've got an in-progress PR that adds an asset_key property. Until that lands, you can do this somewhat verbose thing: next(iter(my_asset.asset_keys))
    @asset_sensor(name=asset.op.name + "_sensor", asset_key=AssetKey(triggering_asset.op.name), job=job_def,
                  default_status=DefaultSensorStatus.RUNNING)
    def my_asset_sensor(context, asset_event):
        records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=AssetKey(triggering_asset.op.name),  # TODO this is a hack - see before
                # asset_partitions=[curr_partition], # do not know - need to get it somewhere? But I do not want to listen - rather want to pass it down from the previous step. One could pass after_timestamp for better performance
            ),
            ascending=False,
            limit=1,
        )
        # TODO: can we somehow get the most recent records guaranteed? (so I do not need to loop all? Could the cursor be used? (i.e. the thing which anyways suppleid the run key))
        # WARNING: in case of out-of-order-execution (backfills) the ordering/triggering is not guaranteed. Any idea how this could be accounted for?
        latest_record = records[0]
        m = latest_record.event_log_entry.dagster_event.step_materialization_data.materialization.metadata_entries
        partition_result = None
        for mm in m:
            if mm.label == 'partition':
                partition_result = mm.entry_data.text
        if partition_result is not None:
            get_dagster_logger().info('partition_completed: in previous step')
        yield RunRequest(
            run_key=context.cursor,
            # TODO where to get this from from triggering_asset? Do I need to filter DagsterEventType.ASSET_MATERIALIZATION and get the partiiton key manually? This seems quite cumbersome
            tags={"latest_partition": partition_result}
        )

    return my_asset_sensor
