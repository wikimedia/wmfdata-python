import os
import pwd
import tempfile
import pandas as pd
import warnings

from pyhive import hive
from shutil import copyfileobj
from wmfdata.utils import (
    check_kerberos_auth,
    ensure_list
)

HIVE_URL = "analytics-hive.eqiad.wmnet"
KERBEROS_SERVICE_NAME = "hive"


def run_cli(
    commands, format="pandas", heap_size=None, use_nice=True,
    use_ionice=True
):
    """
    [DEPRECATED] Formerly ran SQL commands against the Hive tables in the Data Lake
    using Hive's command line interface.  Currently just calls `run`.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is *not*
      supported, and will usually result in an error.
    * `format`: what format to return the results in
        * "pandas": a Pandas data frame
        * "raw": a TSV string, as returned by the command line interface.
    * `heap_size`: [Deprecated]
    * `use_nice`: [Deprecated]
    * `use_ionice`: [Deprecated]
    """
    warnings.warn(
        "'run_cli' is deprecated. It will be removed in the next major release."
        "Please use 'run' instead.",
        category=FutureWarning
    )
    return run(commands, format)


def run(commands, format="pandas", heap_size="deprecated", engine="deprecated"):
    """
    Runs SQL commands against the Hive tables in the Data Lake.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is *not*
      supported, and will usually result in an error.
    * `format`: what format to return the results in
        * "pandas": a Pandas data frame
        * "raw": a TSV string, as returned by the command line interface.
    * `heap_size`: [Deprecated]
    * `engine`: [Deprecated]
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")

    if heap_size != "deprecated":
        warnings.warn(
            "'heap_size' is deprecated. It will be removed in the next major release",
            category=FutureWarning
        )

    if engine != "deprecated":
        warnings.warn(
            "'engine' is deprecated. It will be removed in the next major release.",
            category=FutureWarning
        )

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
            if format == "pandas":
                try:
                    # this will work when the command is a SQL query
                    # so the last query in `commands` will return its results
                    response = pd.read_sql(command, conn)
                except TypeError:
                    # The weird thing here is the command actually runs,
                    # Pandas just has trouble when trying to read the result
                    # So when we pass here, we don't need to re-run the command
                    pass

            elif format == "raw":
                cursor = conn.cursor()
                cursor.execute(command)
                response = cursor.fetchall()

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
    LOAD DATA LOCAL INPATH "{path}"
    OVERWRITE INTO TABLE {db_name}.{table_name}
    """

    try:
        if headers:
            __, tmp_path = tempfile.mkstemp()
            with open(path, 'r') as source, open(tmp_path, 'w') as target:
                # Consume the first line so it doesn't make it to the copy
                source.readline()
                copyfileobj(source, target)
            path = tmp_path

        cmd_params = {
            "db_name": db_name,
            "field_spec": field_spec,
            # To do: Convert relative paths (e.g. "~/data.csv") into absolute paths
            "path": path,
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
        os.unlink(tmp_path)
