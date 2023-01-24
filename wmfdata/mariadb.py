import atexit
import getpass
import grp
import subprocess

import mariadb
from mariadb.constants import FIELD_TYPE
import pandas as pd

from wmfdata.utils import ensure_list


# Set up converter for binary data
def decode_binary(x):
    try:
        return x.decode('utf-8')
    # This should only occur with NULLs and non-binary strings,
    # which don't need conversion
    except AttributeError:
        return x

types_to_decode = [
    FIELD_TYPE.VARCHAR,
    FIELD_TYPE.VAR_STRING,
    FIELD_TYPE.STRING,
    FIELD_TYPE.TINY_BLOB,
    FIELD_TYPE.MEDIUM_BLOB,
    FIELD_TYPE.LONG_BLOB,
    FIELD_TYPE.BLOB
]

converter = {t: decode_binary for t in types_to_decode}


connection=None
# Close any open connections at exit
@atexit.register
def clean_up_connection():
    # The connection variable may not be defined if the connection failed
    # to open
    if connection:
        connection.close()

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

    port = int(host[1])
    host = host[0]

    # Check which group the user is in, and use the appropriate credentials file
    user = getpass.getuser()
    if user in grp.getgrnam("analytics-privatedata-users").gr_mem:
        option_file = "/etc/mysql/conf.d/analytics-research-client.cnf"
    elif user in grp.getgrnam("researchers").gr_mem:
        option_file = "/etc/mysql/conf.d/research-client.cnf"
    # For users in analytics-users, for example
    else:
        raise PermissionError(
            "Your account does not have permission to access the Analytics "
            "MariaDB cluster."
        )

    connection = mariadb.connect(
        host=host,
        port=port,
        db=db,
        default_file=option_file,
        autocommit=True,
        converter=converter
    )

    return connection

# To-do: provide an easy way to get lists of wikis
def run(
  commands, dbs, use_x1=False, format="pandas", date_col=None,
  index_col=None, params=None,
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
        * "centralauth" for global accounts
        * "wikishared" for cross-wiki ExtensionStorage
        * "staging" for user-writable ad-hoc tests and analysis
    * `use_x1`: whether to the connect to the given database on the
      ExtensionStorage replica (only works for wiki databases or "wikishared").
      Default false.
    * `date_col`: this parses the specified column or columns from MediaWiki
      datetimes into Pandas datetimes.
    * `index_col`: passed to pandas.read_sql_query to set a columns or columns
      as the index.
    * `params`: passed to pandas.read_sql_query to provide parameters for the
      SQL query. The MariaDB Python connector uses the qmark parameter style
      specified in PEP 249.
    """

    # Make single command and database parameters lists
    commands = ensure_list(commands)
    dbs = ensure_list(dbs)

    results = []

    for db in dbs:
        connection = connect(db, use_x1)
        result = None

        # Specify the MediaWiki date format for each of the date_cols, if any
        if date_col:
            date_col = ensure_list(date_col)
            date_format = "%Y%m%d%H%M%S"
            date_col = {col: date_format for col in date_col}

        # To-do: SQL syntax errors cause a chain of multiple Python errors
        # The simplest way to fix this is probably to get the raw results and
        # then turn them into a data frame; this would let us avoid using
        # Pandas's complex SQL machinery.
        for command in commands:
            try:
                result = pd.read_sql_query(
                    command,
                    connection,
                    index_col=index_col,
                    parse_dates=date_col,
                    params=params,
                )
            # pandas will encounter a TypeError with DDL (e.g. CREATE TABLE) or
            # DML (e.g. INSERT) statements
            except TypeError:
                pass

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
