import datetime as dt
import os
import re
from shutil import copyfileobj
import subprocess
import tempfile

import pandas as pd
from wmfdata.utils import print_err, mediawiki_dt, check_kerberos_auth

def run_cli(commands, format = "pandas", heap_size = 1024, use_nice = True, use_ionice = True):
    """
    Runs SQL commands against the Hive tables in the Data Lake using Hive's command line interface.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of strings for multiple commands within the same session (useful for things like setting session variables). Passing more than one query is *not* supported, and will usually result in an error.
    * `format`: what format to return the results in
        * "pandas": a Pandas data frame
        * "raw": a TSV string, as returned by the command line interface.
    * `heap_size`: the amount of memory available to the Hive client. Increase this if a command experiences an out of memory error.
    * `use_nice`: Run with a lower priority for processor usage.
    * `use_ionice`: Run with a lower priority for disk access.
    """

    if type(commands) == str:
        commands = [commands]
    if format not in ("pandas", "raw"):
        raise ValueError("'{}' is not a valid format.".format(format))
    check_kerberos_auth()

    shell_command = "export HADOOP_HEAPSIZE={0} && "
    if use_nice:
        shell_command += "/usr/bin/nice "
    if use_ionice:
        shell_command += "/usr/bin/ionice "
    shell_command += "/usr/bin/hive -S -f {1}"

    result = None

    # Support multiple commands by concatenating them in one file. If the user has passed more than one query,
    # this will result in a error when Pandas tries to read the resulting concatenated output (unless the queries
    # happen to produce the same number of columns).
    #
    # Ideally, we would return only the last query's results or throw a clearer error ourselves. However,
    # there's no simple way to determine if multiple queries have been passed or separate their output,
    # so it's not worth the effort.
    merged_commands = ";\n".join(commands)
    
    try:
        # Create temporary files in current working directory to write to:
        cwd = os.getcwd()
        query_fd, query_path = tempfile.mkstemp(suffix=".hql", dir=cwd)
        results_fd, results_path = tempfile.mkstemp(suffix=".tsv", dir=cwd)

        # Write the Hive query:
        with os.fdopen(query_fd, 'w') as fp:
            fp.write(merged_commands)

        # Execute the Hive query:
        shell_command = shell_command.format(heap_size, query_path)
        hive_call = subprocess.run(shell_command, shell=True, stdout=results_fd, stderr=subprocess.PIPE)
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
            # Remove logspam from the standard error so it's easier to see the actual error
            stderr = iter(hive_call.stderr.decode().splitlines())
            cleaned_stderr = ""
            for line in stderr:
                filter = r"JAVA_TOOL_OPTIONS|parquet\.hadoop|WARN:|:WARN|SLF4J"
                if re.search(filter, line) is None:
                    cleaned_stderr += line + "\n"

            raise ChildProcessError(
                "The Hive command line client encountered the following error:\n{}".format(cleaned_stderr)
            )
    finally:
        # Remove temporary files:
        os.unlink(query_path)
        os.unlink(results_path)

    return result

def run(commands, format="pandas", engine="cli"):
    """
    Runs SQL commands against the Hive tables in the Data Lake. Currently, this simply passes the commands to the `run_cli` function.
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")
    if engine not in ("cli"):
        raise ValueError("'{}' is not a valid engine.".format(engine))
    if type(commands) == str:
        commands = [commands]

    result = None
    if engine == "cli":
        return run_cli(commands, format)

def load_csv(
    path, field_spec, db_name, table_name,
    create_db=False, sep=",", headers=True
):
    """
    Upload a CSV (or other delimiter-separated value file) to Data Lake's HDFS, for use with Hive
    and other utilities.

    `field_spec` specifies the field names and their formats, for the
    `CREATE TABLE` statement; for example, `name string, age int, graduated bool`.

    To prevent errors caused by typos, the function will not try to create the database first unless
    `create_db=True` is passed.

    `headers` gives whether the file has a header row; if it does, the function strips it before
    uploading, because Hive treats all rows as data rows.
    """
    if headers:
        new_path = "/tmp/wmfdata-" + mediawiki_dt(dt.datetime.now())
        # From rbtsbg at https://stackoverflow.com/a/39791546
        with open(path, 'r') as source, open(new_path, 'w') as target:
            source.readline()
            copyfileobj(source, target)

        path = new_path

    create_db_cmd = """
    create database if not exists {db_name}
    """.format(
        db_name=db_name
    )

    # To do: Passing a new field spec cannot change an exsting table's format
    create_table_cmd = """
    create table if not exists {db_name}.{table_name} ({field_spec})
    row format delimited fields terminated by "{sep}"
    """.format(
        db_name=db_name, table_name=table_name,
        field_spec=field_spec, sep=sep
    )

    load_table_cmd = """
        load data local inpath "{path}"
        overwrite into table {db_name}.{table_name}
    """.format(
        # To do: Convert relative paths (e.g. "~/data.csv") into absolute paths
        path=path, db_name=db_name,
        table_name=table_name
    )

    if create_db:
        run(create_db_cmd)

    run(create_table_cmd)

    proc = subprocess.Popen(
        ["hive", "-e", load_table_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    try:
        outs, _ = proc.communicate(timeout=15)
        for line in outs.decode().split("\n"):
            print_err(line)
    except TimeoutExpired:
        proc.kill()
        outs, _ = proc.communicate()
        for line in outs.decode().split("\n"):
            print_err(line)
