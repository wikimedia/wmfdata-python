import datetime as dt
from shutil import copyfileobj
import subprocess

from wmfdata.utils import print_err, mediawiki_dt
from wmfdata.spark import get_spark_session

def run(cmds, fmt = "pandas", spark_master='yarn', app_name='wmfdata', spark_config={}):
    """
    Run one or more Hive queries or command on the Data Lake.

    If multiple commands are provided, only results from the last results-producing command will be returned.
    """

    if fmt not in ["pandas", "raw"]:
        raise ValueError("The `fmt` should be either `pandas` or `raw`.")

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

    spark = get_spark_session(spark_master, app_name, spark_config)

    # TODO figure out how to handle multiple commands

    cmd = cmds[0]
    resultDf = spark.sql(cmd)
    if fmt == 'pandas':
        return resultDf.toPandas()
    else:
        return resultDf.collect()

    # for cmd in cmds:
    #     resultDf = spark.sql(cmd)
    #     if fmt == "pandas":
    #         result = resultDf.asPandas()
    #         # Happens if there are no results (as with an INSERT INTO query)
    #         except TypeError:
    #             pass
    #     else:
    #         result = hive_cursor.fetchall()



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
