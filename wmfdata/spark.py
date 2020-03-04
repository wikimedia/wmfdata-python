from threading import Timer

import findspark
findspark.init('/usr/lib/spark2')
from pyspark.sql import SparkSession

from wmfdata.utils import check_kerberos_auth
             
REGULAR_SPARK_SETTINGS = {
    "spark.driver.memory": "2g",
    "spark.dynamicAllocation.maxExecutors": 64,
    "spark.executor.memory": "8g",
    "spark.executor.cores": 4,
    "spark.sql.shuffle.partitions": 256
}

LARGE_SPARK_SETTINGS = {
    "spark.driver.memory": "4g",
    "spark.dynamicAllocation.maxExecutors": 128,
    "spark.executor.memory": "8g",
    "spark.executor.cores": 4,
    "spark.sql.shuffle.partitions": 512
}

EXTRA_JAVA_OPTIONS = {
    "http.proxyHost": "webproxy.eqiad.wmnet",
    "http.proxyPort": "8080",
    "https.proxyHost": "webproxy.eqiad.wmnet",
    "https.proxyPort": "8080"
}

def get_application_id(session):
    return session.sparkContext.applicationId

# A cache mapping Spark sessions to off-main-thread timers stopping them after an hour.
session_timeouts = {}

def cancel_session_timeout(session):
    """
    Checks whether a timeout is set for a particular Spark session and cancels it if it exists.
    """
    
    application_id = get_application_id(session)
    
    timeout = session_timeouts.get(application_id)
    if timeout:
        timeout.cancel()
        # Delete the stopped thread from `session_timeouts` as a sign that the session was not stopped
        del session_timeouts[application_id]

def start_session_timeout(session):
    """
    Starts an off-main-thread timer stopping a Spark session after an hour.
    """
    application_id = get_application_id(session)
    
    # Cancel any existing timeout on the same session
    cancel_session_timeout(session)

    # When the timeout executes, leave the stopped thread in `session_timeouts` as a sign that the session was stopped
    timeout = Timer(3600, session.stop)
    session_timeouts[application_id] = timeout
    timeout.start()

def get_session(type="regular", app_name="wmfdata", extra_settings={}):
    """
    Returns an existent Spark session, or a new one if one hasn't yet been created.
    
    Code calling this is responsible for starting a timeout using `start_session_timeout` when it finishes using the session, in order to prevent idle Spark sessions from wasting cluster resources. This function takes cares of cancelling the timeout if the session is returned again before the timeout finishes.
    """
    
    if type not in ("regular", "large"):
        raise ValueError("'{}' is not a valid Spark session type.".format(type))

    check_kerberos_auth()

    # TODO: if there's an existing session, it will be returned with its existing settings even if 
    # the user has specified a different set of settings in this function call. There will be no indication
    # that this has happened.
    builder = (
        SparkSession.builder
        .master("yarn")
        .appName(app_name)
        .config(
            "spark.driver.extraJavaOptions",
            " ".join("-D{}={}".format(k, v) for k, v in EXTRA_JAVA_OPTIONS.items())
        )
    )

    if type == "regular":
        config = REGULAR_SPARK_SETTINGS
    elif type == "large":
        config = LARGE_SPARK_SETTINGS
    # Add in any extra settings, overwriting if applicable
    config.update(extra_settings)
    for k, v in config.items():
        builder.config(k, v)

    session = builder.getOrCreate()
    
    # Cancel any existing timeout on the same session, in case an existing session is being returned
    cancel_session_timeout(session)
    
    return session

def run(commands, format="pandas", session_type="regular", extra_settings={}):
    """
    Runs SQL commands against the Hive tables in the Data Lake using the PySpark SQL interface.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of strings for multiple commands within the same session (useful for things like setting session variables). Passing more than one query is *not* supported; only results from the second will be returned.
    * `format`: the format in which to return data
        * "pandas": a Pandas data frame
        * "raw": a list of tuples, as returned by the Spark SQL interface.
    * `session_type`: the type of Spark session to create.
        * "regular": the default; able to use up to 15% of Hadoop cluster resources
        * "large": for queries which require more processing (e.g. joins) or which access more data; able to use up to 30% of Hadoop cluster resources.
    * `extra_settings`: A dict of additional settings to use when creating the Spark session. These will override the defaults specified by `session_type`.
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")
    if session_type not in ("regular", "large"):
        raise ValueError("'{}' is not a valid Spark session type.".format(session_type))
    if type(commands) == str:
        commands = [commands]
    
    result = None
    # TODO: Switching the Spark session type has no effect if the previous session is still running
    spark_session = get_session(type=session_type, extra_settings=extra_settings)
    for cmd in commands:
        cmd_result = spark_session.sql(cmd)
        # If the result has columns, the command was a query and therefore results-producing.
        # If not, it was a DDL or DML command and not results-producing.
        if len(cmd_result.columns) > 0:
            uncollected_result = cmd_result
    if uncollected_result and format == "pandas":
        result = uncollected_result.toPandas()
    elif format == "raw":
        result = uncollected_result.collect()

    start_session_timeout(spark_session)
    
    return result
