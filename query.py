import pandas as pd
import pymysql
from impala.dbapi import connect as impala_conn
from impala.util import as_pandas

# Strings are stored in MariaDB as BINARY rather than CHAR/VARCHAR, so they need to be converted.
def try_decode(cell):
    try:
        return cell.decode(encoding = "utf-8")
    except AttributeError:
        return cell

def decode_data(d):
    return [{try_decode(key): try_decode(val) for key, val in item.items()} for item in d]


def run_mariadb(*cmds, fmt = "pandas"):
    """
    Used to run an SQL query or command on the `analytics-store` MariaDB replica. 
    Multiple commands can be specified as multiple positional arguments, in which case only the result
    from the final results-producing command will be returned.
    """

    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")

    try:
        conn = pymysql.connect(
            # To-do: We need to be able to query the EventLogging host too
            host = "analytics-store.eqiad.wmnet",
            read_default_file = '/etc/mysql/conf.d/research-client.cnf',
            charset = 'utf8mb4',
            db='staging',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit = True
        )
        
        result = None
        
        # Overwrite the result during each iteration so only the last result is retured
        for cmd in cmds:
            if fmt == "raw":
                cursor = conn.cursor()
                cursor.execute(cmd)
                result = cursor.fetchall()
                result = decode_data(result)
            else:
                try:
                    result = pd.read_sql_query(cmd, conn)
                    # Turn any binary data into strings
                    result = result.applymap(try_decode)
                # pandas will encounter a TypeError with DDL (e.g. CREATE TABLE) or DML (e.g. INSERT) statements
                except TypeError:
                    pass

        return result

    finally:
        conn.close()

            
# To-do: allow for multiple commands as with `run_mariadb()`
# To-do: figure out how to use the `fmt` parameter when calling a magic
def run_hive(cmd, fmt = "pandas"):
    """Used to run a Hive query or command on the Data Lake stored on the Analytics cluster."""
    
    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")
    
    result = None
    
    try:
        hive_conn = impala_conn(host='analytics1003.eqiad.wmnet', port=10000, auth_mechanism='PLAIN')
        hive_cursor = hive_conn.cursor()
        hive_cursor.execute(cmd)
        if fmt == "pandas":
            try:
                result = as_pandas(hive_cursor)
            # Happens if there are no results (as with an INSERT INTO query)
            except TypeError:
                pass
        else:
            result = hive_cursor.fetchall()    
    finally:
        hive_conn.close()
    
    return result
