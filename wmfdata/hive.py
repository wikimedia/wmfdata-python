import os
import pwd
import tempfile
import pandas as pd

from pyhive import hive
from shutil import copyfileobj
from wmfdata.utils import (
    ensure_list
)

HIVE_URL = 'analytics-hive.eqiad.wmnet'
KERBEROS_SERVICE_NAME = 'hive'


def run_cli(
    commands, format="pandas", heap_size=1024, use_nice=True,
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
    * `heap_size`: the amount of memory available to the Hive client. Increase
      this if a command experiences an out of memory error.
    * `use_nice`: [Deprecated]
    * `use_ionice`: [Deprecated]
    """
    return run(commands, format, heap_size)


def run(commands, format="pandas", heap_size=1024):
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
    * `heap_size`: the amount of memory available to the Hive client. Increase
      this if a command experiences an out of memory error.
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")

    # Support multiple commands by concatenating them with ";". If the user
    # has passed more than one query, this will result in a error when Pandas
    # tries to read the resulting concatenated output (unless the queries
    # happen to produce the same number of columns).
    #
    # Ideally, we would return only the last query's results or throw a clearer
    # error ourselves. However, there's no simple way to determine if multiple
    # queries have been passed or separate their output, so it's not worth
    # the effort.
    commands = ";\n".join(ensure_list(commands))

    connect_kwargs = {
        'host': HIVE_URL,
        'auth': 'KERBEROS',
        'username': pwd.getpwuid(os.getuid()).pw_name,
        'kerberos_service_name': KERBEROS_SERVICE_NAME,
    }
    with hive.connect(**connect_kwargs) as conn:
        if format == "pandas":
            return pd.read_sql(commands, conn)
        if format == "raw":
            cursor = conn.cursor()
            cursor.execute(commands)
            return cursor.fetchall()



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
            run_cli(create_db_cmd.format(**cmd_params))
        run_cli([
            drop_table_cmd.format(**cmd_params),
            create_table_cmd.format(**cmd_params),
            load_table_cmd.format(**cmd_params)
        ])
    finally:
        os.unlink(tmp_path)
