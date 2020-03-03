import datetime as dt
import os
import re
from shutil import copyfileobj
import subprocess
import tempfile

import pandas as pd
from wmfdata import spark
from wmfdata.utils import print_err, mediawiki_dt, check_kerberos_auth

def run_on_cli(cmds, fmt = "pandas", heap_size = 1024, use_nice = True, use_ionice = True):
    """
    Run a query using the Hive command line interface
    """

    if type(cmds) == str:
        cmds = [cmds]
    if fmt not in ("pandas", "raw"):
        raise ValueError("'{}' is not a valid format.".format(fmt))
    check_kerberos_auth()

    shell_command = "export HADOOP_HEAPSIZE={0} && "
    if use_nice:
        shell_command += "/usr/bin/nice "
    if use_ionice:
        shell_command += "/usr/bin/ionice "
    shell_command += "/usr/bin/hive -S -f {1}"

    result = None

    # Support multiple commands by concatenating them in one file.
    # If the user has provided two queries, this will result in broken output.
    # But that is not an supported use case, and supporting the previous behavior of run() 
    # (returning only the output of the second query) would be very complex. Similarly,
    # trying to detect if multiple queries have been provided would be very complex.
    
    merged_cmds = ";\n".join(cmds)
    try:
        # Create temporary files in current working directory to write to:
        cwd = os.getcwd()
        query_fd, query_path = tempfile.mkstemp(suffix=".hql", dir=cwd)
        results_fd, results_path = tempfile.mkstemp(suffix=".tsv", dir=cwd)

        # Write the Hive query:
        with os.fdopen(query_fd, 'w') as fp:
            fp.write(merged_cmds)

        # Execute the Hive query:
        shell_command = shell_command.format(heap_size, query_path)
        hive_call = subprocess.run(shell_command, shell=True, stdout=results_fd, stderr=subprocess.PIPE)
        if hive_call.returncode == 0:
            # Read the results upon successful execution of cmd:
            if fmt == "pandas":
                result = pd.read_csv(results_path, sep='\t')
            else:
                # If user requested "raw" results, read the text file as-is:
                with open(results_path, 'r') as file:
                    result = file.read()
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
        # Cleanup:
        os.unlink(query_path)
        os.unlink(results_path)

    return result

def run(cmds, fmt="pandas", engine="cli"):
    """
    Run one or more Hive queries or command on the Data Lake.

    If multiple commands are provided, only results from the last results-producing command will be returned.
    """

    if fmt not in ["pandas", "raw"]:
        raise ValueError("The `fmt` should be either `pandas` or `raw`.")
    if engine not in ("cli"):
        raise ValueError("'{}' is not a valid engine.".format(engine))
    if type(cmds) == str:
        cmds = [cmds]

    result = None
    if engine == "cli":
        return run_on_cli(cmds, fmt)

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
