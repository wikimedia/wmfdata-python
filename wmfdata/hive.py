import datetime as dt
import os
import re
from shutil import copyfileobj
import subprocess
import tempfile

import pandas as pd
from wmfdata import spark
from wmfdata.utils import print_err, mediawiki_dt, check_kerberos_auth

def run_on_cli(query, fmt = "pandas", heap_size = 1024, use_nice = True, use_ionice = True):
    """
    Run a query using the Hive command line interface
    """

    if fmt not in ("pandas", "raw"):
        raise ValueError("'{}' is not a valid format.".format(fmt))
    check_kerberos_auth()

    cmd = "export HADOOP_HEAPSIZE={0} && "

    if use_nice:
        cmd += "/usr/bin/nice "

    if use_ionice:
        cmd += "/usr/bin/ionice "

    cmd += "/usr/bin/hive -S -f {1}"

    results = None
    try:
        # Create temporary files in current working directory to write to:
        cwd = os.getcwd()
        query_fd, query_path = tempfile.mkstemp(suffix=".hql", dir=cwd)
        results_fd, results_path = tempfile.mkstemp(suffix=".tsv", dir=cwd)

        # Write the Hive query:
        with os.fdopen(query_fd, 'w') as fp:
            fp.write(query)

        # Execute the Hive query:
        cmd = cmd.format(heap_size, query_path, results_path)
        hive_call = subprocess.run(cmd, shell=True, stdout=results_fd, stderr=subprocess.PIPE)
        if hive_call.returncode == 0:
            # Read the results upon successful execution of cmd:
            if fmt == "pandas":
                results = pd.read_csv(results_path, sep='\t')
            else:
                # If user requested "raw" results, read the text file as-is:
                with open(results_path, 'r') as file:
                    results = file.read()
        # If the hive call has not completed successfully
        else:
            # Remove logspam from the standard error so it's easier to see the actual error
            stderr = iter(hive_call.stderr.decode().splitlines())
            cleaned_stderr = ""
            for line in stderr:
                filter = r"JAVA_TOOL_OPTIONS|parquet\.hadoop|WARN:|:WARN|SLF4J"
                if re.search(filter, line) is None:
                    cleaned_stderr += line
                    cleaned_stderr += "\n"

            raise ChildProcessError(
                "The Hive command line client encountered the following error:\n{}".format(cleaned_stderr)
            )
    finally:
        # Cleanup:
        os.unlink(query_path)
        os.unlink(results_path)

    return results

def run(cmds, fmt="pandas", engine="hive-cli", spark_config={}, extra_spark_settings={}):

    """
    Run one or more Hive queries or command on the Data Lake.

    If multiple commands are provided, only results from the last results-producing command will be returned.
    """

    if fmt not in ["pandas", "raw"]:
        raise ValueError("The `fmt` should be either `pandas` or `raw`.")
    if engine not in ("hive-cli", "spark", "spark-large"):
        raise ValueError("'{}' is not a valid engine.".format(engine))
    if type(cmds) == str:
        cmds = [cmds]
    # `spark_config` deprecated in Feb 2020
    if spark_config:
        extra_spark_settings = spark_config
        print_err("The `spark_config` parameter has been deprecated. Please use the `extra_spark_settings` parameter instead.")
    
    result = None

    if engine in ("spark", "spark-large"):
        # TODO: Switching the Spark session type has no effect if the previous session is still running
        if engine == "spark":
            spark_session = spark.get_session(type="regular", extra_settings=extra_spark_settings)
        elif engine == "spark-large":
            spark_session = spark.get_session(type="large", extra_settings=extra_spark_settings)
        
        for cmd in cmds:
            cmd_result = spark_session.sql(cmd)
            # If the result has columns, the command was a query and therefore results-producing.
            # If not, it was a DDL or DML command and not results-producing.
            if len(cmd_result.columns) > 0:
                uncollected_result = cmd_result
        if uncollected_result and fmt == "pandas":
            result = uncollected_result.toPandas()
        elif fmt == "raw":
            result = uncollected_result.collect()

        spark.start_session_timeout(spark_session)
    elif engine == "hive-cli":
        for cmd in cmds:
            cmd_result = run_on_cli(cmd, fmt)
            if cmd_result is not None:
                if fmt == "pandas":
                    result = cmd_result
                else:
                    # TODO: this should be in the same format as other raw results
                    result = cmd_result

    return result

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
