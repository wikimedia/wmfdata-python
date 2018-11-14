import datetime as dt
from shutil import copyfileobj
import subprocess

from impala.dbapi import connect as impala_conn
from impala.util import as_pandas as impala_as_pd
from wmfdata.utils import print_err, mediawiki_dt
            
def run(cmds, fmt = "pandas"):
    """
    Run one or more Hive queries or command on the Data Lake.
    
    If multiple commands are provided, only results from the last results-producing command will be returned.
    """
    
    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")
    
    if type(cmds) == str:
        cmds = [cmds]
    
    result = None
    
    try:
        hive_conn = impala_conn(host='an-coord1001.eqiad.wmnet', port=10000, auth_mechanism='PLAIN')
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
    
    return result

def upload_csv(
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
