import pandas as pd
from impala.dbapi import connect as impala_conn
from impala.util import as_pandas
            
# To-do: allow for multiple commands as with `mariadb()`
def run(cmd, fmt = "pandas"):
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
