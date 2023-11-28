import os
import warnings

import pandas as pd
import prestodb
from requests.packages import urllib3

from wmfdata.utils import (
    check_kerberos_auth,
    ensure_list
)

# Disable a warning issued by urllib3 because the certificate for an-coord1001.eqiad.wmnet
# does not specify the hostname in the subjectAltName field (T158757)
SubjectAltNameWarning = urllib3.exceptions.SubjectAltNameWarning
urllib3.disable_warnings(SubjectAltNameWarning)

def run(commands, catalog="analytics_hive"):
    """
    Runs one or more SQL commands using the Presto SQL engine and returns the last result
    in a Pandas dataframe.
    
    Presto can` be connected to many different backend data catalogs. Currently, it is only connected to the Data Lake, which has the catalog name "analytics_hive".

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is not
      supported; only results from the second will be returned.
    """
    commands = ensure_list(commands)
    check_kerberos_auth()

    USER_NAME = os.getenv("USER")
    PRESTO_AUTH = prestodb.auth.KerberosAuthentication(
        config="/etc/krb5.conf",
        service_name="presto",
        principal=f"{USER_NAME}@WIKIMEDIA",
        ca_bundle="/etc/ssl/certs/wmf-ca-certificates.crt"
    )

    connection = prestodb.dbapi.connect(
        catalog=catalog,
        host="an-coord1001.eqiad.wmnet",
        port=8281,
        http_scheme="https",
        user=USER_NAME,
        auth=PRESTO_AUTH,
        source=f"{USER_NAME}, wmfdata-python"
    )

    cursor = connection.cursor()
    final_result = None
    for command in commands:
        cursor.execute(command)
        result = cursor.fetchall()
        description = cursor.description

        # Weirdly, this happens after running a command that doesn't produce results (like a
        # CREATE TABLE or INSERT). Most users can't run those, though.
        # TO-DO: report this as a bug upstream
        if result == [[True]] and description[0][0] == "results":
            pass
        else:
            # Based on
            # https://github.com/prestodb/presto-python-client/issues/56#issuecomment-367432438
            colnames = [col[0] for col in description]
            dtypes = [col[1] for col in description]
            def setup_transform(col, desired_dtype):
                # Only Hive dates/times need special handling
                if desired_dtype in ("timestamp", "date"):
                    return lambda df: pd.to_datetime(df[col])
                else:
                    return lambda df: df[col]

            transformations = {
                col: setup_transform(col, dtype)
                for col, dtype in zip(colnames, dtypes)
            }
            
            final_result = (
                pd.DataFrame(result, columns=colnames)
                .assign(**transformations)
            )
 
    cursor.cancel()
    connection.close()

    return final_result

