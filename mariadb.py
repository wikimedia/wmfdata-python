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

def run(cmds, fmt = "pandas", host = "wikis"):
    """
    Used to run SQL queries or commands on the analytics MariaDB replicas. 
    
    A single command can be specified as a string or multiple commands as a list of strings.
    
    If multiple commands are provided, only the results from the final results-producing command are returned
    
    The host can be "wikis" to run on the wiki replicas or "logs" to run on the EventLogging host.
    
    The format can be "pandas", returning a Pandas data frame, or "raw", returning a list of tuples.
    """

    if host == "wikis":
        full_host = "analytics-store.eqiad.wmnet"
    elif host == "logs":
        full_host = "analytics-slave.eqiad.wmnet"
    else:
        full_host = host
        
    if type(cmds) == str:
        cmds = [cmds]
    
    if fmt not in ["pandas", "raw"]:
        raise ValueError("The format should be either `pandas` or `raw`.")

    result = None
    
    try:
        conn = mysql.connect(
            host = full_host,
            option_files = '/etc/mysql/conf.d/research-client.cnf',
            charset = 'binary',
            database ='staging',
            autocommit = True
        )
        
        # It's valuable for this function to support multiple commands so that it's possible to run
        # multiple commands within the same connection.
        for cmd in cmds:
            if fmt == "raw":
                cursor = conn.cursor()
                cursor.execute(cmd)
                try:
                    result = cursor.fetchall()
                    result = decode_data(result)
                    
                #mysql-connector-python will encounter an InterfaceError if there are no results to fetch
                except mysql.errors.InterfaceError:
                    pass

            else:
                try:
                    result = pd.read_sql_query(cmd, conn)
                    # Turn any binary data and column names into strings
                    result = result.applymap(try_decode).rename(columns = try_decode)                  
                
                # pandas will encounter a TypeError with DDL (e.g. CREATE TABLE) or DML (e.g. INSERT) statements
                except TypeError:
                    pass

        return result

    finally:
        conn.close()

def multirun(cmds, wikis = utils.list_wikis()):
    if type(cmds) == str:
        cmds = [cmds]
    
    result = None
    
    for wiki in wikis:
        init = time.perf_counter()
        
        use_cmd = ["use {db}".format(db = wiki)]
        
        part_result = run(use_cmd + cmds)
        
        if result is None:
            result = part_result
        else:
            result = pd.concat([result, part_result], ignore_index = True)
        
        elapsed = time.perf_counter() - init
        utils.print_err("{} completed in {:0.0f} s".format(wiki, elapsed))
        
    return result
