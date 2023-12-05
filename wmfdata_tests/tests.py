#!/usr/bin/env python3
import argparse
import io
import os
from pathlib import Path
import re
import sys

import pandas as pd
import wmfdata as wmf

this_directory = str(Path(__file__).parent.resolve())

# To do: This way of specifying the Hive database makes it impossible to
# import this as a module. It would be better to have the test functions
# take the database as an argument, and look for and pass a command line
# argument only if it's being run as a script.
parser = argparse.ArgumentParser()
parser.add_argument(
    "hive_database",
    help=
        "The Hive database where test tables will be written. It's best to "
        "choose your own database."
)

args = parser.parse_args()
hive_db = args.hive_database

# Theoretically, the passed database could be used to inject SQL (e.g. passing
# 'wmf.webrequest --' in an attempt to drop webrequest). Practically, it
# cannot be since analytics cluster users are all trusted and, unless they are
# data engineers, only have write access to their own and explicitly shared
# tables. Still, it doesn't hurt to close the hole by checking that the passed
# database is a valid SQL identifier.
if re.fullmatch(r"[a-zA-z][a-zA-Z0-9_]*", hive_db) is None:
    raise ValueError(f"`{hive_db}` is not a valid database.")

# This dataset contains strings, int, and datetimes
TEST_DATA_1 = pd.read_parquet(f"{this_directory}/test_data_1.parquet", engine="pyarrow")

TEST_DATA_2 = pd.read_csv(f"{this_directory}/test_data_2.csv")

def log_test_passed(test_name):
    print(f"TEST PASSED: {test_name}")

def assert_dataframes_match(df1, df2):
    assert df1.equals(df2)
    assert df1.index.equals(df2.index)
    assert df1.columns.equals(df2.columns)

def set_up_table_1():
    spark = wmf.spark.create_session(type="local", app_name="wmfdata-test")
    spark_df = spark.read.load(f"file://{this_directory}/test_data_1.parquet")
    spark_df.write.mode("overwrite").saveAsTable(f"{hive_db}.wmfdata_test_1")

def set_up_mariadb_table():
    test_data_tuples = TEST_DATA_1.itertuples(index=False, name=None)
    test_data_1_records = ",\n".join(
        [str(t) for t in test_data_tuples]
    )

    CREATE_TABLE_SQL = """
        CREATE TABLE wmfdata_test (
            month VARCHAR(255),
            wiki VARCHAR(255),
            user_id BIGINT,
            user_name VARCHAR(255),
            edits BIGINT,
            content_edits BIGINT,
            user_registration VARCHAR(255)
        )
    """

    INSERT_RECORDS_SQL = f"""
        INSERT INTO
        wmfdata_test
        VALUES
        {test_data_1_records}
    """

    wmf.mariadb.run(
        [CREATE_TABLE_SQL, INSERT_RECORDS_SQL],
        dbs=["staging"]
    )

def clean_tables():
    wmf.hive.run([
      f"DROP TABLE IF EXISTS {hive_db}.wmfdata_test_1",
      f"DROP TABLE IF EXISTS {hive_db}.wmfdata_test_2",
      f"DROP TABLE IF EXISTS {hive_db}.wmfdata_test_empty",
    ])
    wmf.mariadb.run("DROP TABLE IF EXISTS wmfdata_test", dbs="staging")


READ_TABLE_1 = f"""
    SELECT *
    FROM {hive_db}.wmfdata_test_1
    ORDER BY user_name
    LIMIT 1000
"""

READ_TABLE_2 = f"""
    SELECT *
    FROM {hive_db}.wmfdata_test_2
    ORDER BY iso_code
    LIMIT 1000
"""

# We want to ensure this correctly returns None, without an error.
# Wmfdata has seen several such bugs!
SILENT_COMMAND = "DROP TABLE IF EXISTS wmfdata_test_abcdefghjkl"

def test_read_via_hive():
    test_data_1_via_hive = wmf.hive.run(READ_TABLE_1)
    assert_dataframes_match(TEST_DATA_1, test_data_1_via_hive)
    log_test_passed("Read via Hive")

def test_silent_command_via_hive():
   output = wmf.hive.run(SILENT_COMMAND)
   assert output is None
   log_test_passed("Silent command via Hive")

def test_spark_session_apis():
    # once create_session() is called, we should be able to retrieve it via get_active_session()
    s1 = wmf.spark.create_session(type="local", app_name="wmfdata-test-session-api-1")
    assert wmf.spark.get_active_session() is s1
    # a later call to create_session() closes any previous session and returns a new one
    s2 = wmf.spark.create_session(type="local", app_name="wmfdata-test-session-api-2")
    assert s1 is not s2
    assert wmf.spark.get_active_session() is s2
    # clean up
    s2.stop()
    log_test_passed("Spark Session APIs")

def test_read_via_spark():
    test_data_1_via_spark = wmf.spark.run(READ_TABLE_1)
    assert_dataframes_match(TEST_DATA_1, test_data_1_via_spark)
    log_test_passed("Read via Spark")

def test_silent_command_via_spark():
   output = wmf.spark.run(SILENT_COMMAND)
   assert output is None
   log_test_passed("Silent command via Spark")

def test_read_via_presto():
    test_data_1_via_presto = wmf.presto.run(READ_TABLE_1)
    assert_dataframes_match(TEST_DATA_1, test_data_1_via_presto)
    log_test_passed("Read via Presto")

# Our Presto setup does not support DDL or DML commands, so the issue
# of silent commands causing an error doesn't come up

def test_read_via_mariadb():
    test_data_1_via_mariadb = wmf.mariadb.run(
        """
        SELECT *
        FROM wmfdata_test
        """,
        "staging"
    )

    assert_dataframes_match(TEST_DATA_1, test_data_1_via_mariadb)

    log_test_passed("Read via MariaDB")

def test_silent_command_via_MariaDB():
   output = wmf.mariadb.run(SILENT_COMMAND, dbs="staging")
   assert output is None
   log_test_passed("Silent command via MariaDB")

def test_load_csv():
    wmf.hive.load_csv(
        f"{this_directory}/test_data_2.csv",
        "name string, iso_code string, economic_region string, maxmind_continent string",
        db_name=hive_db,
        table_name="wmfdata_test_2"
    )

    test_data_2 = wmf.hive.run(READ_TABLE_2)
    assert_dataframes_match(TEST_DATA_2, test_data_2)

    log_test_passed("Load CSV to Data Lake")

def test_sql_tuple():
    try:
        wmf.utils.sql_tuple([])
    # We want a ValueError in this case
    except ValueError:
        pass
    else:
        raise AssertionError("Passing an empty iterable to sql_tuple should raise a ValueError.")

    t = wmf.utils.sql_tuple(("enwiki", "arwiki", "dewiki"))
    assert t == "('enwiki', 'arwiki', 'dewiki')"

    log_test_passed("utils.sql_tuple unit test")

def test_df_to_remarkup():
    # Test to see that we can remove indexes and format numbers in df_to_remarkup outputs.
    df_requests_dummy = pd.DataFrame(
        data={
            "http_status": [200, 404, 305],
            "total_requests": [975, 20, 2],
            "percent_of_total": [97.5, 2, 0.5]
        }
    )

    # Capture the output of df_to_remarkup print statements and test their values.
    captured_output_1 = io.StringIO()
    sys.stdout = captured_output_1
    wmf.utils.df_to_remarkup(df_requests_dummy)
    output_str_1 = captured_output_1.getvalue().split("\n")[:-1]

    # We have the default `index=True`, so the first value of row 0 should be the index value `0`.
    assert output_str_1[2].split("|")[1].strip() == "0"
    # We are not using floatfmt by default, so the percent in the second row should be the integer `2`.
    assert output_str_1[3].split("|")[-2].strip() == "2"

    captured_output_2 = io.StringIO()
    sys.stdout = captured_output_2
    wmf.utils.df_to_remarkup(df_requests_dummy, index=False, floatfmt=(".0f", ".0f", ".1f"))
    output_str_2 = captured_output_2.getvalue().split("\n")[:-1]

    # We have `index=False`, so the first value of row 0 should be the `http_status` value `200`.
    # It should also be `200` and not `200.0` as this column has been formatted to `.0f` using floatfmt.
    assert output_str_2[2].split("|")[1].strip() == "200"
    # floatfmt was used to round the third column to the first decimal place, so this percent should be `2.0`.
    assert output_str_2[3].split("|")[-2].strip() == "2.0"

    log_test_passed("utils.df_to_remarkup unit tests")

def main():
    clean_tables()
    set_up_table_1()
    set_up_mariadb_table()

    test_read_via_hive()
    test_silent_command_via_hive()
    test_spark_session_apis()
    test_read_via_spark()
    test_silent_command_via_spark()
    test_read_via_presto()
    test_read_via_mariadb()
    test_silent_command_via_MariaDB()
    test_load_csv()
    test_sql_tuple()
    test_df_to_remarkup()

    clean_tables()

if __name__ == "__main__":
    main()
