from impala.dbapi import connect as impala_conn
from impala.util import as_pandas as impala_as_pd
            
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
