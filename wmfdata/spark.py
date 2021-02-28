from threading import Timer
import warnings
import os

import findspark

from wmfdata import conda
from wmfdata.utils import (
    check_kerberos_auth,
    ensure_list,
    python_version,
    log
)

"""
Will be used for findspark.init().
"""
SPARK_HOME = os.environ.get("SPARK_HOME", "/usr/lib/spark2")

"""
Predefined spark sessions and configs for use with the get_session and run functions.
"""
PREDEFINED_SPARK_SESSIONS = {
    "local": {
        "master": "local[2]",  # [2] means use 2 local worker threads
        "config": {
            "spark.driver.memory": "4g",
        }
    },
    "yarn-regular": {
        "master": "yarn",
        "config": {
            "spark.driver.memory": "2g",
            "spark.dynamicAllocation.maxExecutors": 64,
            "spark.executor.memory": "8g",
            "spark.executor.cores": 4,
            "spark.sql.shuffle.partitions": 256
        }
    },
    "yarn-large": {
        "master": "yarn",
        "config": {
            "spark.driver.memory": "4g",
            "spark.dynamicAllocation.maxExecutors": 128,
            "spark.executor.memory": "8g",
            "spark.executor.cores": 4,
            "spark.sql.shuffle.partitions": 512
        }
    }
}

# Add previous session "type" keys for backwards compatibility.
PREDEFINED_SPARK_SESSIONS["regular"] = PREDEFINED_SPARK_SESSIONS["yarn-regular"]
PREDEFINED_SPARK_SESSIONS["large"] = PREDEFINED_SPARK_SESSIONS["yarn-large"]

EXTRA_JAVA_OPTIONS = {
    "http.proxyHost": "webproxy.eqiad.wmnet",
    "http.proxyPort": "8080",
    "https.proxyHost": "webproxy.eqiad.wmnet",
    "https.proxyPort": "8080"
}

def get_custom_session(
    master="local[2]",
    app_name="wmfdata-custom",
    spark_config={},
    ship_python_env = False,
    conda_pack_kwargs = {}
):
    """
    Returns an existent SparkSession, or a new one if one hasn't yet been created.

    Use this instead of get_session if you'd rather have manual control over
    your SparkSession configuration.

    Note: master, app_name and spark_config are only applied the first time
    this function is called.  All subsequent calls will return the first created SparkSession.

    Arguments:
    * `master`: passed to SparkSession.builder.master()
      If this is "yarn" and and a conda env is active and and ship_python_env=False,
      remote executors will be configured to use conda.conda_base_env_prefix(),
      which defaults to anaconda-wmf. This should usually work as anaconda-wmf
      is installed on all WMF YARN worker nodes.  If your conda environment
      has required packages installed that are not in anaconda-wmf, set
      ship_python_env=True.
    * `app_name`: passed to SparkSession.builder.appName().
    * `spark_config`: passed to SparkSession.builder.config()
    * `ship_python_env`: If master='yarn' and this is True, a conda env will be packed
      and shipped to remote Spark executors.  This is useful if your conda env
      has Python or other packages that the executors will need to do their work.
    * `conda_pack_kwargs`: Args to pass to conda_pack.pack(). If none are given, this will
      call conda_pack.pack() with no args, causing the default currently active
      conda environment to be packed.
      You can pack and ship any conda environment by setting appropriate args here.
      See https://conda.github.io/conda-pack/api.html#pack
      If True, this will fail if conda and conda_pack are not installed.
    """
    if master == "yarn":
        check_kerberos_auth()

        if ship_python_env:
            # The path to our packed conda environment.
            conda_packed_file = conda.pack(**conda_pack_kwargs)
            # This will be used as the unpacked directory name in the YARN working directory.
            conda_packed_name = os.path.splitext(os.path.basename(conda_packed_file))[0]

            # Ship conda_packed_file to each YARN worker.
            conda_spark_archive = f"{conda_packed_file}#{conda_packed_name}"
            if "spark.yarn.dist.archives" in spark_config:
                spark_config["spark.yarn.dist.archives"] += f",{conda_spark_archive}"
            else:
                spark_config["spark.yarn.dist.archives"] = conda_spark_archive
            log.info(f"Will ship {conda_packed_file} to remote Spark executors.")

            # Workers should use python from the unpacked conda env.
            os.environ["PYSPARK_PYTHON"] = f"{conda_packed_name}/bin/python3"
        # Else if conda is active, use the use the conda_base_env_prefix (anaconda-wmf)
        # environment, as this should exist on all worker nodes.
        elif conda.is_active():
            os.environ["PYSPARK_PYTHON"] = os.path.join(
                conda.conda_base_env_prefix(), "bin", "python3"
            )
        # Else use the system python.  We can't use any current conda or virtualenv python
        # as these won't be present on the remote YARN workers.
        # The python version workers should use must be the same as the currently
        # running python version, so only set this if that version of python
        # (e.g. python3.7) is installed in the system.
        elif os.path.isfile(os.path.join(f"/usr/bin/python{python_version()}")):
            os.environ["PYSPARK_PYTHON"] = f"/usr/bin/python{python_version()}"

        if "PYSPARK_PYTHON" in os.environ:
            log.info(
                "PySpark executors will use {}.".format(os.environ["PYSPARK_PYTHON"])
            )

    # NOTE: We don't need to touch PYSPARK_PYTHON if master != yarn.
    # The default set by findspark will be fine.

    # Call findspark.init after PYSPARK_PYTHON has possibly been set.
    # This is needed because findspark will set PYSPARK_PYTHON path
    # to sys.executable if it isn't yet set, which will likely not
    # work in YARN mode if sys.executlable is a local conda or virtualenv
    # (as is the case in WMF Jupyter Notebooks).
    findspark.init(SPARK_HOME)
    from pyspark.sql import SparkSession

    # TODO: if there's an existing session, it will be returned with its
    # existing settings even if the user has specified a different set of
    # settings in this function call. There will be no indication that
    # this has happened.
    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config(
            "spark.driver.extraJavaOptions",
            " ".join(
              "-D{}={}".format(k, v) for k, v in EXTRA_JAVA_OPTIONS.items()
            )
        )
    )

    # Apply any provided spark configs.
    for k, v in spark_config.items():
        builder.config(k, v)

    return builder.getOrCreate()

def get_session(
    type="yarn-regular",
    app_name=None,
    extra_settings={},
    ship_python_env=False,
):
    """
    Returns a Spark session based on one of the PREDEFINED_SPARK_SESSION types.

    Arguments:
    * `type`: the type of Spark session to create.
        * "local": Run the command in a local Spark process. Use this for
          prototyping or querying small-ish data (less than a couple of GB).
        * "yarn-regular": the default; able to use up to 15% of Hadoop cluster
          resources (This is the default).
        * "yarn-large": for queries which require more processing (e.g. joins) or
          which access more data; able to use up to 30% of Hadoop cluster
          resources.
    * `extra_settings`: A dict of additional Spark configs to use when creating
      the Spark session. These will override the defaults specified
      by `type`.
    * `ship_python_env`: If master='yarn' and this is True, a conda env will be packed
      and shipped to remote Spark executors.  This is useful if your active conda env
      has Python or other packages that the executors will need to do their work.
    """

    if type not in PREDEFINED_SPARK_SESSIONS.keys():
        raise ValueError(
            "'{}' is not a valid predefined Spark session type. Must be one of {}".format(
                type, PREDEFINED_SPARK_SESSIONS.keys()
            )
        )
    if app_name is None:
        app_name = "wmfdata-{}".format(type)

    config = PREDEFINED_SPARK_SESSIONS[type]["config"]
    # Add in any extra settings, overwriting if applicable
    config.update(extra_settings)

    return get_custom_session(
        master=PREDEFINED_SPARK_SESSIONS[type]["master"],
        app_name=app_name,
        spark_config=config,
        ship_python_env=ship_python_env
    )


def get_application_id(session):
    return session.sparkContext.applicationId

# A cache mapping Spark sessions to off-main-thread timers stopping them
# after an hour.
session_timeouts = {}

def cancel_session_timeout(session):
    """
    Checks whether a timeout is set for a particular Spark session
    and cancels it if it exists.
    """

    application_id = get_application_id(session)

    timeout = session_timeouts.get(application_id)
    if timeout:
        timeout.cancel()
        # Delete the stopped thread from `session_timeouts` as a sign
        # that the session was not stopped.
        del session_timeouts[application_id]


def stop_session(session):
    """
    Cancels any session timers and stops the SparkSession.
    """
    cancel_session_timeout(session)
    session.stop()

def start_session_timeout(session, timeout_seconds=3600):
    """
    Starts an off-main-thread timer stopping a Spark session after an hour.
    """
    application_id = get_application_id(session)

    # Cancel any existing timeout on the same session
    cancel_session_timeout(session)

    # When the timeout executes, leave the stopped thread in `session_timeouts`
    # as a sign that the session was stopped.
    timeout = Timer(timeout_seconds, stop_session)
    session_timeouts[application_id] = timeout
    timeout.start()

def run(commands, format="pandas", session_type="yarn-regular", extra_settings={}):
    """
    Runs SQL commands against the Hive tables in the Data Lake using the
    PySpark SQL interface.

    Note: The session_type and extra_settings will only be applied
    the first time this function is called.  The SparkSession is only instantiated
    once per process, and each subsequent call will re-use the previously
    created SparkSession.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is *not*
      supported; only results from the second will be returned.
    * `format`: the format in which to return data
        * "pandas": a Pandas data frame
        * "raw": a list of tuples, as returned by the Spark SQL interface.
    * `session_type`: the type of Spark session to create.
        * "local": Run the command in a local Spark process. Use this for
          prototyping or querying small-ish data (less than a couple of GB).
        * "yarn-regular": the default; able to use up to 15% of Hadoop cluster
          resources
        * "yarn-large": for queries which require more processing (e.g. joins) or
          which access more data; able to use up to 30% of Hadoop cluster
          resources.
    * `extra_settings`: A dict of additional settings to use when creating
      the Spark session. These will override the defaults specified
      by `session_type`.
    """

    if format not in ["pandas", "raw"]:
        raise ValueError("The `format` should be either `pandas` or `raw`.")
    if format == "raw":
        warnings.warn(
            "The 'raw' format is deprecated. It will be removed in the next major release.",
            category=FutureWarning
        )

    commands = ensure_list(commands)

    result = None
    # TODO: Switching the Spark session type has no effect if the previous
    # session is still running.
    spark_session = get_session(
      type=session_type,
      extra_settings=extra_settings
    )
    for cmd in commands:
        cmd_result = spark_session.sql(cmd)
        # If the result has columns, the command was a query and therefore
        # results-producing. If not, it was a DDL or DML command and not
        # results-producing.
        if len(cmd_result.columns) > 0:
            uncollected_result = cmd_result
    if uncollected_result and format == "pandas":
        result = uncollected_result.toPandas()
    elif format == "raw":
        result = uncollected_result.collect()

    # (re)start a timeout on SparkSessions in Yarn after the result is collected.
    # A SparkSession used by this run function
    # will timeout after 5 minutes, unless used again.
    if PREDEFINED_SPARK_SESSIONS[session_type]["master"] == "yarn":
        start_session_timeout(spark_session, 3600)

    return result
