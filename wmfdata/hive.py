import datetime as dt
from shutil import copyfileobj
import subprocess

from impala.dbapi import connect as impala_conn
from impala.util import as_pandas as impala_as_pd
from wmfdata.utils import print_err, mediawiki_dt

import os
import tempfile
import pandas as pd

def hive_cli(query, heap_size = 1024, use_nice = True, use_ionice = True):
    """
    Run a query using the Hive command line interface
    """
    cmd = "export HADOOP_HEAPSIZE={0} && "

    if use_nice:
        cmd += "/usr/bin/nice "

    if use_ionice:
        cmd += "/usr/bin/ionice "

    cmd += "/usr/bin/hive -S -f {1} 2> /dev/null"

    filters = ["JAVA_TOOL_OPTIONS", "parquet.hadoop", "WARN:", ":WARN"]
    for filter in filters:
        cmd += " | grep -v " + filter

    cmd += " > {2}"

    results = None
    try:
        # Create temporary files to write to:
        query_fd, query_path = tempfile.mkstemp(suffix=".hql")
        results_fd, results_path = tempfile.mkstemp(suffix=".tsv")

        # Write the Hive query:
        with os.fdopen(query_fd, 'w') as fp:
            fp.write(query)

        # Execute the Hive query:
        cmd = cmd.format(heap_size, query_path, results_path)
        hive_call = subprocess.call(cmd)
        if hive_call = 0:
            # Read the results upon successful execution of cmd:
            results = pd.read_csv(results_path, sep='\t')
    finally:
        # Cleanup:
        os.unlink(query_path)
        os.unlink(results_path)

    return results

def run(cmds, fmt = "pandas", via = "impala"):
    """
    Run one or more Hive queries or command on the Data Lake.

    If multiple commands are provided, only results from the last results-producing command will be returned.
    """

    if fmt not in ["pandas", "raw"]:
        raise ValueError("The `fmt` should be either `pandas` or `raw`.")

    if via not in ["impala", "cli"]:
        raise ValueError("The `via` should be either `impala` or `cli`.")

    if type(cmds) == str:
        cmds = [cmds]

    # Check whether user has authenticated with Kerberos:
    klist = subprocess.call("klist")
    if klist == 1:
        raise OSError(
            "You do not have Kerberos credentials. " +
            "Authenticate using `kinit` or run your script as a keytab-enabled user."
        )
    elif klist != 0:
        raise OSError("There was an unknown issue checking your Kerberos credentials.")

    result = None

    if via == "impala":
        try:
            hive_conn = impala_conn(
                host='an-coord1001.eqiad.wmnet',
                port=10000,
                auth_mechanism='GSSAPI',
                kerberos_service_name='hive'
            )
            hive_cursor = hive_conn.cursor()

            for cmd in cmds:
                hive_cursor.execute(cmd)
                if fmt == "pandas":
                    try:
                        result = impala_as_pd(hive_cursor)
                    # Happens if there are no results (as with an INSERT INTO query)
                    except TypeError:
                        pass
                else:
                    result = hive_cursor.fetchall()
        finally:
            hive_conn.close()
    else:
        for cmd in cmds:
            r = hive_cli(cmd)
            if result is not None and r is not None:
                result.append(r)
            else:
                result = r

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
