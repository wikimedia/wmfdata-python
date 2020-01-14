from threading import Timer

import findspark
findspark.init('/usr/lib/spark2')
from pyspark.sql import SparkSession

# TODO:
# Auto zip and ship juptyer venv with yarn spark job.
# https://wikitech.wikimedia.org/wiki/SWAP#Launching_as_SparkSession_in_a_Python_Notebook

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
        # The stopped thread remains in the dictionary
        del session_timeouts[application_id]

def start_session_timeout(session):
    """
    Starts an off-main-thread timer stopping a Spark session after an hour.
    """
    application_id = get_application_id(session)
    
    # Cancel any existing timeout on the same session
    cancel_session_timeout(session)

    timeout = Timer(3600, session.stop)
    session_timeouts[application_id] = timeout
    timeout.start()

def get_session(master='yarn', app_name='wmfdata', spark_config={}):
    """
    Returns an existent Spark session, or a new one if one hasn't yet been created.
    
    Code calling this is responsible for starting a timeout using `start_session_timeout` when it finishes using the session, in order to prevent idle Spark sessions from wasting cluster resources. This function takes cares of cancelling the timeout if the session is returned again before the timeout finishes.
    """

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config(
            'spark.driver.extraJavaOptions',
            ' '.join('-D{}={}'.format(k, v) for k, v in {
                'http.proxyHost': 'webproxy.eqiad.wmnet',
                'http.proxyPort': '8080',
                'https.proxyHost': 'webproxy.eqiad.wmnet',
                'https.proxyPort': '8080',
            }.items()))
    )

    for k, v in spark_config.items():
        builder.config(k, v)

    session = builder.getOrCreate()
    
    # Cancel any existing timeout on the same session, in case an existing session is being returned
    cancel_session_timeout(session)
    
    return session
