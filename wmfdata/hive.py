import os
import pwd
import tempfile
import pandas as pd
import subprocess
import warnings

from pyhive import hive
from shutil import copyfileobj
from wmfdata.utils import (
    check_kerberos_auth,
    ensure_list
)

HIVE_URL = "analytics-hive.eqiad.wmnet"
KERBEROS_SERVICE_NAME = "hive"


def run(commands):
    """
    Runs SQL commands against the Hive tables in the Data Lake.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is *not*
      supported, and will usually result in an error.
    """

    check_kerberos_auth()

    connect_kwargs = {
        "host": HIVE_URL,
        "auth": "KERBEROS",
        "username": pwd.getpwuid(os.getuid()).pw_name,
        "kerberos_service_name": KERBEROS_SERVICE_NAME,
    }

    commands = ensure_list(commands)
    response = None

    with hive.connect(**connect_kwargs) as conn:
        for command in commands:
            try:
                # this will work when the command is a SQL query
                # so the last query in `commands` will return its results
                with warnings.catch_warnings():
                    message="pandas only supports SQLAlchemy connectable"
                    warnings.filterwarnings("ignore", category=UserWarning, message=message)
                    response = pd.read_sql(command, conn)
            except TypeError:
                # The weird thing here is the command actually runs,
                # Pandas just has trouble when trying to read the result
                # So when we pass here, we don't need to re-run the command
                pass

    return response


def load_csv(
    path, field_spec, db_name, table_name,
    create_db=False, sep=",", headers=True
):
    """
    Upload a CSV (or other delimiter-separated value file) to Data Lake's HDFS,
    for use with Hive and other utilities.

    `field_spec` specifies the field names and their formats, for the
    `CREATE TABLE` statement; for example, `name string, age int, graduated
    bool`.

    To prevent errors caused by typos, the function will not try to create the
    database first unless `create_db=True` is passed.

    `headers` gives whether the file has a header row; if it does, the
    function strips it before uploading, because Hive treats all rows as
    data rows.
    """

    create_db_cmd = """
    CREATE DATABASE IF NOT EXISTS {db_name}
    """

    drop_table_cmd = """
    DROP TABLE IF EXISTS {db_name}.{table_name}
    """

    create_table_cmd = """
    CREATE TABLE {db_name}.{table_name} ({field_spec})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "{sep}"
    STORED AS TEXTFILE
    """

    load_table_cmd = """
    LOAD DATA INPATH "hdfs://{path}"
    OVERWRITE INTO TABLE {db_name}.{table_name}
    """

    try:
        __, tmp_path = tempfile.mkstemp()
        if headers:
            with open(path, 'r') as source, open(tmp_path, 'w') as target:
                # Consume the first line so it doesn't make it to the copy
                source.readline()
                copyfileobj(source, target)
            path = tmp_path

        # Copy the file to HDFS because pyhive runs via JDBC not off the local client
        hdfs_path = f"{path}"
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path])
        subprocess.run(["hdfs", "dfs", "-put", path, hdfs_path])

        cmd_params = {
            "db_name": db_name,
            "field_spec": field_spec,
            # To do: Convert relative paths (e.g. "~/data.csv") into absolute paths
            "path": hdfs_path,
            "sep": sep,
            "table_name": table_name
        }

        if create_db:
            run(create_db_cmd.format(**cmd_params))
        run([
            drop_table_cmd.format(**cmd_params),
            create_table_cmd.format(**cmd_params),
            load_table_cmd.format(**cmd_params)
        ])
    finally:
        if tmp_path:
            os.unlink(tmp_path)
