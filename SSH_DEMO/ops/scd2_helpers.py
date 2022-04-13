from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql.functions import when, lead, lag, col, asc, current_date, exp, mean, log1p
from pyspark.sql.window import Window
from functools import reduce, partial
import re


def cast_decimal_to_double(df):
    """
    Convert any column of type `Decimal` to `DoubleType`
    https://stackoverflow.com/questions/58048745/how-to-detect-if-decimal-columns-should-be-converted-into-integer-or-double"""
    decimal_to_double = lambda x: re.sub(r'decimal\(\d+,\d+\)', 'double', x)
    return df.select(
        *[col(d[0]).astype(decimal_to_double(d[1])) if 'decimal' in d[1] else col(d[0]) for d in df.dtypes])


def deduplicate_start(e, window_primary_key):
    """
    2 cases: 1.: start (previous is NULL), 2: in between, try to collapse
    first, filter to only start & end events (= updates/invalidations of records)
    :param e: the column operated on
    :param window_primary_key: the window used for partitioning
    :return: start event columns
    """
    previous = lag(col(e), 1).over(window_primary_key)
    current = col(e)
    return (current != previous) | previous.isNull()


def deduplicate_end(e, window_primary_key):
    """
    2 cases: 1.: in between, try to collapse 2: end (= next is null)
    first, filter to only start & end events (= updates/invalidations of records)
    :param e: the column operated on
    :param window_primary_key: the window used for partitioning
    :return: end event columns
    """
    next_elem = lead(col(e), 1).over(window_primary_key)
    current = col(e)
    return (current != next_elem) | next_elem.isNull()


def deduplicate_scd2(key, sort_changing_ignored, time_column, columns_to_ignore, df):
    """
    SCD2-style the metadata tables first
    Based on https://georgheiler.com/2020/11/19/sparkling-scd2/,
    https://stackoverflow.com/questions/59586700/implement-scd-type-2-in-spark
    :param key: key to partition by
    :param sort_changing_ignored: change ignored, used to order by  (time, batch_id, ...)
    :param time_column: column representing the time of the event (i.e. day)
    :param columns_to_ignore:  columns to not consider (i.e. they are all null)
    :param df: the raw data to be SCD2 cleaned
    :return: for each key one row for each attribute value change with `valid_from`/`valid_to` columns
    """
    window_primary_key = Window.partitionBy(*map(col, key)).orderBy(*map(col, sort_changing_ignored))
    columns_to_compare = df.drop(*key).drop(*sort_changing_ignored).drop(*columns_to_ignore).columns
    next_data_change = lead(time_column, 1).over(window_primary_key)

    deduplicated = df.withColumn("data_changes_start",
                                 reduce((lambda x, y: x | y),
                                        map(partial(deduplicate_start, window_primary_key=window_primary_key),
                                            columns_to_compare))).withColumn("data_changes_end",
                                                                             reduce((lambda x, y: x | y),
                                                                                    map(partial(deduplicate_end,
                                                                                                window_primary_key=window_primary_key),
                                                                                        columns_to_compare))).filter(
        col("data_changes_start") | col("data_changes_end")).drop("data_changes")

    return deduplicated.withColumn("valid_to",
                                   when(col("data_changes_end") == True, col(time_column)).otherwise(
                                       next_data_change)).filter(
        col("data_changes_start") == True).withColumn("valid_to",
                                                      when(next_data_change.isNull(), current_date()).otherwise(
                                                          col("valid_to"))).withColumnRenamed(time_column,
                                                                                              "valid_from").drop(
        "data_changes_end", "data_changes_start")


