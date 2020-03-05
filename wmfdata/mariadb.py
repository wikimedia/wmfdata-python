import atexit
from collections import namedtuple
from itertools import chain
import subprocess

# https://pypi.org/project/mysql-connector-python/
import mysql.connector as mysql
import pandas as pd

from wmfdata.utils import print_err

# Close any open connections at exit
@atexit.register
def clean_up_connection():
    # The connection variable may not be defined if the connection failed
    # to open
    if connection:
        connection.close()

# Useful for allowing an argument to take a single string or a list of strings
def ensure_list(str_or_list):
    if type(str_or_list) == str:
        return [str_or_list]
    else:
        return str_or_list

def connect(db, use_x1=False):
    # The `analytics-mysql` script requires users to know that the `wikishared`
    # database is located on x1.
    if db == "wikishared":
        use_x1 = True
    
    host_command = "analytics-mysql {db} --print-target".format(db=db)
    if use_x1:
        host_command = host_command + " --use-x1"
    
    host = subprocess.run(
        host_command, 
        shell=True,
        stdout=subprocess.PIPE,
        universal_newlines=True
    ).stdout.strip().split(":")
    
    if host == ['']:
        raise ValueError("The database '{}' was not found.".format(db))
    
    port = host[1]
    host = host[0]
        
    connection = mysql.connect(
        host=host,
        port=port,
        db=db,
        option_files='/etc/mysql/conf.d/research-client.cnf',
        charset='utf8',
        autocommit=True
    )
    
    return connection

def run_to_pandas(connection, commands, date_col=None, index_col=None):
    result = None
    
    # Specify the MediaWiki date format for each of the date_cols, if any
    if date_col:
        date_col = ensure_list(date_col)
        date_format = "%Y%m%d%H%M%S"
        date_col = {col: date_format for col in date_col}
    
    # To-do: SQL syntax errors cause a chain of multiple Python errors
    for command in commands:
        try:
            result = pd.read_sql_query(
              command, connection, index_col=index_col, parse_dates=date_col
            )
        # pandas will encounter a TypeError with DDL (e.g. CREATE TABLE) or
        # DML (e.g. INSERT) statements
        except TypeError:
            pass

    return result

# A named tuple type for returning tuples-format results
ResultSet = namedtuple("ResultSet", ["column_names", "records"])

def run_to_tuples(connection, commands):
    result = None
    cursor = connection.cursor()

    for command in commands:
        cursor.execute(command)
        if cursor.with_rows:
            records = cursor.fetchall()
            column_names = [x[0] for x in cursor.description]
            result = ResultSet(column_names, records)

    return result

# To-do: provide an easy way to get lists of wikis
def run(
  commands, dbs, use_x1=False, format="pandas", date_col=None,
  index_col=None
):
    """
    Run SQL queries or commands on the Analytics MediaWiki replicas.
    
    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables).
    * `dbs`: a string for one database or a list to run the commands on
      multiple databases and concatenate the results.  Possible values:
        * a wiki's database code (e.g. "enwiki", "arwiktionary", "wikidatawiki")
          for its MediaWiki database (or its ExtensionStorage database if
          `use_x1` is passed)
        * "logs" for the EventLogging
        * "centralauth" for global accounts
        * "wikishared" for cross-wiki ExtensionStorage 
        * "staging" for user-writable ad-hoc tests and analysis
    * `use_x1`: whether to the connect to the given database on the
      ExtensionStorage replica (only works for wiki databases or "wikishared").
      Default false.
    * `format`: which format to return the data in. "pandas" (the default) means
      a Pandas DataFrame, "tuples" means a named tuple consisting of (1) the
      columns names and (2) the records as a list of tuples, the raw format
      specified by Python's database API specification v2.0.
    * `date_col`: if using Pandas format, this parses the specified column or
      columns from MediaWiki datetimes into Pandas datetimes. If using tuples
      format, has no effect.
    * `index_col`: if using Pandas format, passed to pandas.read_sql_query to
      set a columns or columns as the index. If using tuples format, has no
      effect.
    """
    
    # Make single command and database parameters lists
    commands = ensure_list(commands)
    dbs = ensure_list(dbs)
    
    results = []
        
    if format == "pandas":
        for db in dbs:
            connection = connect(db, use_x1)
            result = run_to_pandas(connection, commands, date_col, index_col)
            connection.close()
            results.append(result)
        
        if len(dbs) > 1:
            # Ignore the indexes on the partial results unless a custom index
            # column was designated
            if not index_col:
                ignore_index = True
            else:
                ignore_index = False
                
            return pd.concat(results, ignore_index=ignore_index)
        else:
            return results[0]
    
    # Allow "raw" as a synonym of "tuples" for temporary back-compatibility
    # (July 2019)
    elif format == "tuples" or format == "raw":
        if format == "raw":
            print_err(
              "The 'raw' format has been renamed 'tuples'. Please use the new "
              "name instead."
            )
            
        for db in dbs:
            connection = connect(db, use_x1)
            result = run_to_tuples(connection, commands)
            connection.close()
            results.append(result)

        if len(dbs) > 1:
            # Take the first set of column names since they'll all be the same
            column_names = results[0].column_names
            
            record_sets = [result.records for result in results]
            records = [x for x in chain(record_sets)]
            
            return ResultSet(column_names, records)
        else:
            return results[0]
    
    else:
        raise ValueError("The format you specified is not supported.")

def multirun(cmds, wikis = None):
    print_err(
       "The multirun function has been deprecated. Please pass a list of "
       "databases to the run function instead."
    )
    
    if not wikis:
        raise NotImplementedError(
          "The default set of wikis to run the command on have been removed. "
          "Please explicitly specify a list of wikis."
        )
    
    return run(cmds, wikis)