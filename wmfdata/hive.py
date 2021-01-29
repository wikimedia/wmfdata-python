import datetime as dt
import os
import re
from shutil import copyfileobj
import subprocess
import tempfile

import pandas as pd
from wmfdata.utils import (
    check_kerberos_auth, ensure_list, mediawiki_dt, print_err
)

def run_cli(
  commands, format = "pandas", heap_size = 1024, use_nice = True,
  use_ionice = True
):
    """
    Runs SQL commands against the Hive tables in the Data Lake using Hive's
    command line interface.

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
    * `use_nice`: Run with a lower priority for processor usage.
    * `use_ionice`: Run with a lower priority for disk access.
    """

    commands = ensure_list(commands)
    if format not in ["pandas", "raw"]:
        raise ValueError("'{}' is not a valid format.".format(format))
    check_kerberos_auth()

    shell_command = "export HADOOP_HEAPSIZE={0} && "
    if use_nice:
        shell_command += "/usr/bin/nice "
    if use_ionice:
        shell_command += "/usr/bin/ionice "
    shell_command += "/usr/bin/hive -S -f {1}"

    result = None

    # Support multiple commands by concatenating them in one file. If the user
    # has passed more than one query, this will result in a error when Pandas
    # tries to read the resulting concatenated output (unless the queries
    # happen to produce the same number of columns).
    #
    # Ideally, we would return only the last query's results or throw a clearer
    # error ourselves. However, there's no simple way to determine if multiple
    # queries have been passed or separate their output, so it's not worth
    # the effort.
    merged_commands = ";\n".join(commands)
    
    try:
        # Create temporary files to hold the query and result
        query_fd, query_path = tempfile.mkstemp(suffix=".hql")
        results_fd, results_path = tempfile.mkstemp(suffix=".tsv")

        # Write the Hive query:
        with os.fdopen(query_fd, 'w') as fp:
            fp.write(merged_commands)

        # Execute the Hive query:
        shell_command = shell_command.format(heap_size, query_path)
        hive_call = subprocess.run(
          shell_command,
          shell=True,
          stdout=results_fd,
          stderr=subprocess.PIPE
        )
        if hive_call.returncode == 0:
            # Read the results upon successful execution of cmd:
            if format == "pandas":
                try:
                    result = pd.read_csv(results_path, sep='\t')
                except pd.errors.EmptyDataError:
                    # The command had no output
                    pass
            else:
                # If user requested "raw" results, read the text file as-is:
                with open(results_path, 'r') as file:
                    content = file.read()
                    # If the statement had output:
                    if content:
                        result = content
        # If the hive call has not completed successfully
        else:
            # Remove logspam from the standard error so it's easier to see
            # the actual error
            stderr = iter(hive_call.stderr.decode().splitlines())
            cleaned_stderr = ""
            for line in stderr:
                filter = r"JAVA_TOOL_OPTIONS|parquet\.hadoop|WARN:|:WARN|SLF4J"
                if re.search(filter, line) is None:
                    cleaned_stderr += line + "\n"

            raise ChildProcessError(
                "The Hive command line client encountered the following "
                "error:\n{}".format(cleaned_stderr)
            )
    finally:
        # Remove temporary files:
        os.unlink(query_path)
        os.unlink(results_path)

    return result

def run(commands, format="pandas", engine="cli"):
    """
    Runs SQL commands against the Hive tables in the Data Lake. Currently,
    this simply passes the commands to the `run_cli` function.
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")
    if engine not in ["cli"]:
        raise ValueError("'{}' is not a valid engine.".format(engine))
    commands = ensure_list(commands)

    result = None
    if engine == "cli":
        return run_cli(commands, format)

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
