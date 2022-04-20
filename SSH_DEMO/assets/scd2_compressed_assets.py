import pyspark
from dagster import asset
from pyspark.sql import functions as F

from SSH_DEMO.ops.scd2_helpers import deduplicate_scd2

TIME_COLUMN = 'dt'
ignored_cols = ['event_dt', 'load_ts']
DB_ZONE = "staging"


@asset(
    compute_kind="spark",
    # partitions_def=daily_partitions_def,
    metadata={"key": ["foo"], "sort_changing_ignored": [TIME_COLUMN], "time_column": TIME_COLUMN,
              "columns_to_ignore": ignored_cols, "db_zone": DB_ZONE},
    required_resource_keys={"pyspark"},
)
def foo_scd2_asset(context, foo_asset: pyspark.sql.DataFrame):
    return _shared_helper_scd2(context, foo_asset)


@asset(
    compute_kind="spark",
    # partitions_def=daily_partitions_def,
    metadata={"key": ["foo"], "sort_changing_ignored": [TIME_COLUMN], "time_column": TIME_COLUMN,
              "columns_to_ignore": ignored_cols, "db_zone": DB_ZONE},
    required_resource_keys={"pyspark"},
)
def bar_scd2_asset(context, bar_asset: pyspark.sql.DataFrame):
    return _shared_helper_scd2(context, bar_asset)


@asset(
    compute_kind="spark",
    # partitions_def=daily_partitions_def,
    metadata={"key": ["foo"], "sort_changing_ignored": [TIME_COLUMN], "time_column": TIME_COLUMN,
              "columns_to_ignore": ignored_cols, "db_zone": DB_ZONE},
    required_resource_keys={"pyspark"},
)
def baz_scd2_asset(context, baz_asset: pyspark.sql.DataFrame):
    return _shared_helper_scd2(context, baz_asset)


def _shared_helper_scd2(context, input_asset: pyspark.sql.DataFrame):
    def _get_scd2_config(context):
        return (
            context.solid_def.output_defs[0].metadata["key"],
            context.solid_def.output_defs[0].metadata["sort_changing_ignored"],
            context.solid_def.output_defs[0].metadata["time_column"],
            context.solid_def.output_defs[0].metadata["columns_to_ignore"]
        )

    key, sort_changing_ignored, time_column, columns_to_ignore = _get_scd2_config(context)

    # fixup data types from yyyyMMdd to actual date type!
    input_asset = input_asset.withColumn(time_column, F.to_date(F.col(time_column).cast("string"), "yyyyMMdd"))
    dummy_s_scd2 = deduplicate_scd2(key=key, sort_changing_ignored=sort_changing_ignored, time_column=time_column,
                                    columns_to_ignore=columns_to_ignore, df=input_asset)
    dummy_s_scd2.printSchema()
    # dummy_s_scd2.show()
    return dummy_s_scd2
