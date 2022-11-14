from typing import List, Union
import warnings
import os

import findspark
import pandas as pd

from wmfdata import conda
from wmfdata.utils import (
    check_kerberos_auth,
    ensure_list,
    print_err,
    python_version
)


if conda.is_anaconda_wmf_env():
    default_spark = "/usr/lib/spark2"
else:
    default_spark = "/usr/lib/spark3"
SPARK_HOME = os.environ.get("SPARK_HOME", default_spark)

# This is not necessary in a conda-analytics environment. Once we stop supporting anaconda-wmf
# environments, we can drop this line and the dependency on findspark.
findspark.init(SPARK_HOME)

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

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

# Any environment variables here will be set in all Spark processes
# via spark.executorEnv and spark.yarn.appMasterEnv settings.
ENV_VARS_TO_PROPAGATE = [
    # Always propagate http proxy settings.
    # -Djava.net.useSystemProxies=true should be set in extraJavaOptions for this to work.
    # WMF sets java.net.useSystemProxies=true in spark-defaults.conf, so we don't need to set it here.
    'http_proxy',
    'https_proxy',
    'no_proxy',
]

def get_active_session():
    """
    Returns the active session if there is one and None otherwise.

    Spark 3 includes a native getActiveSession function. Once we end Spark 2
    support, we can switch to that.
    """
    if SparkContext._jvm:
        if SparkContext._jvm.SparkSession.active():
            return SparkSession.builder.getOrCreate()

def create_custom_session(
    master="local[2]",
    app_name="wmfdata-custom",
    spark_config={},
    ship_python_env=False,
    conda_pack_kwargs={}
):
    """
    Creates a new Spark session, stopping any existing session first.

    Use this instead of create_session if you'd rather have manual control over
    your SparkSession configuration.

    Arguments:
    * `master`: passed to SparkSession.builder.master()
      If this is "yarn" and and a conda env is active and and ship_python_env=False,
      remote executors will be configured to use conda.conda_base_env_prefix(),
      which for Spark2 defaults to anaconda-wmf and for Spark3 defalts to conda-analytics.
      This should usually work as both are installed on all WMF YARN worker nodes.
      If your conda environment has required packages installed that are not in those, set
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
    check_kerberos_auth()

    if SPARK_HOME == "/usr/lib/spark2":
        warnings.warn(
            "\n    Spark 2 has been deprecated. Please migrate to Spark 3."
            "\n    See https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Spark/"
            "Migration_to_Spark_3",
            category=FutureWarning
        )

    if master == "yarn":
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
            print_err(f"Will ship {conda_packed_file} to remote Spark executors.")

            # Workers should use python from the unpacked conda env.
            os.environ["PYSPARK_PYTHON"] = f"{conda_packed_name}/bin/python3"
        # Else if conda is active, use the conda_base_env_prefix
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

    # NOTE: We don't need to touch PYSPARK_PYTHON if master != yarn.
    # The default set by findspark will be fine.

    sc = SparkContext._active_spark_context
    if sc:
        sc.stop()

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
    )

    # All ENV_VARS_TO_PROPAGATE should be set in all Spark processes.
    for var in ENV_VARS_TO_PROPAGATE:
        if var in os.environ:
            builder.config(f"spark.executorEnv.{var}", os.environ[var])
            # NOTE: Setting the var in appMasterEnv will only have an effect if
            # running in yarn cluster mode.
            builder.config(f"spark.yarn.appMasterEnv.{var}", os.environ[var])

    # Apply any provided spark configs.
    for k, v in spark_config.items():
        builder.config(k, v)

    return builder.getOrCreate()

def create_session(
    type="yarn-regular",
    app_name=None,
    extra_settings={},
    ship_python_env=False,
):
    """
    Creates a new Spark session based on one of the PREDEFINED_SPARK_SESSION
    types, stopping any existing session first.

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

    return create_custom_session(
        master=PREDEFINED_SPARK_SESSIONS[type]["master"],
        app_name=app_name,
        spark_config=config,
        ship_python_env=ship_python_env
    )


def run(commands: Union[str, List[str]]) -> pd.DataFrame:
    """
    Runs SQL commands against the Hive tables in the Data Lake using the
    PySpark SQL interface.

    Note: this command will use the existing Spark session if there is one and
    otherwise create a predefined "yarn-regular" session. If you want to use
    a different type of session, use `create_session` or `create_custom_session`
    first.

    Note: this function loads all the output into memory on the client. If
    your command produces many gigabytes of output, it could cause an
    out-of-memory error.

    Arguments:
    * `commands`: the SQL to run. A string for a single command or a list of
      strings for multiple commands within the same session (useful for things
      like setting session variables). Passing more than one query is *not*
      supported; only results from the second will be returned.
    """

    commands = ensure_list(commands)

    session = get_active_session()
    if not session:
        session = create_session() 

    overall_result = None

    for cmd in commands:
        cmd_result = session.sql(cmd)
        # If the result has columns, the command was a query and therefore
        # results-producing. If not, it was a DDL or DML command and not
        # results-producing.
        if len(cmd_result.columns) > 0:
            overall_result = cmd_result

    if overall_result:
        overall_result = overall_result.toPandas()

    return overall_result
