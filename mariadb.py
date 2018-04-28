import time

import mysql.connector as mysql # `pip install mysql-connector-python`
import pandas as pd

from . import utils

# Strings are stored in MariaDB as BINARY rather than CHAR/VARCHAR, so they need to be converted.
def try_decode(cell):
    try:
        return cell.decode(encoding = "utf-8")
    except AttributeError:
        return cell

def decode_data(l):
    return [
        tuple(try_decode(v) for v in t) 
        for t in l
    ]


def run(*cmds, fmt = "pandas", host = "wikis"):
    """
    Used to run an SQL query or command on the `analytics-store` MariaDB replica. 
    Multiple commands can be specified as multiple positional arguments, in which case only the result
    from the final results-producing command will be returned.
    """

    if host == "wikis":
        full_host = "analytics-store.eqiad.wmnet"
    elif host == "logs":
        full_host = "analytics-slave.eqiad.wmnet"
    else:
        full_host = host
    
    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")

    try:
        conn = mysql.connect(
            host = full_host,
            option_files = '/etc/mysql/conf.d/research-client.cnf',
            charset = 'binary',
            database ='staging',
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

def multirun(*cmds, wikis = utils.list_all_wikis()):
    result = None
    
    for wiki in wikis:
        init = time.perf_counter()
        
        part_result = run(
            "use {db}".format(db = wiki),
            *cmds
        )
        
        if result is None:
            result = part_result
        else:
            result = pd.concat([result, part_result], ignore_index = True)
        
        elapsed = time.perf_counter() - init
        utils.print_err("{} completed in {:0.0f} s".format(wiki, elapsed))
        
    return result
