import pandas as pd
import pyspark
from dagster import get_dagster_logger, asset


@asset(dagster_type=pd.DataFrame)  # (io_manager_key="parquet_io_manager")
# @asset(required_resource_keys={"pyspark"}, )  # (io_manager_key="parquet_io_manager")
def combined_asset(context, foo_scd2_asset: pyspark.sql.DataFrame, bar_scd2_asset: pyspark.sql.DataFrame,
                   baz_scd2_asset: pyspark.sql.DataFrame):
    get_dagster_logger().info(
        f"updating combined asset (globally for all partitions) once all 3 input assets for a specific partition_key (date) are done")

    # TODO add some processing logic here perhaps instead via DBT for joining it all up - or perhaps pyspark directly
    df = pd.DataFrame({'foo': [1, 2, 3]})
    # return context.resources.pyspark.spark_session.createDataFrame(df)
    return df
