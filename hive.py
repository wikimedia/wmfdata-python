from impala.dbapi import connect as impala_conn
from impala.util import as_pandas as impala_as_pd
            
def run(cmds, fmt = "pandas"):
    """Used to run a Hive query or command on the Data Lake stored on the Analytics cluster."""
    
    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")
    
    if type(cmds) == str:
        cmds = [cmds]
    
    results = []
    
    try:
        hive_conn = impala_conn(host='analytics1003.eqiad.wmnet', port=10000, auth_mechanism='PLAIN')
        hive_cursor = hive_conn.cursor()
        
        for cmd in cmds:
            hive_cursor.execute(cmd)
            if fmt == "pandas":
                try:
                    results.append(impala_as_pd(hive_cursor))
                # Happens if there are no results (as with an INSERT INTO query)
                except TypeError:
                    results.append(None)
            else:
                results.append(hive_cursor.fetchall()) 
    finally:
        hive_conn.close()
    
    return results
