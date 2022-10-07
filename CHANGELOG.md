# Next version
* `mariadb.run` now uses the MariaDB Python connector library rather than the MySQL one, which fixes several errors ([T319360](https://phabricator.wikimedia.org/T319360)).

# 1.3.3 (11 March 2022)
* Improve handling of nulls and blobs in binary fields (#29)
* Switch to pyhive as a hive interface (#24)

# 1.3.2 (2 February 2022)
* `mariadb.run` now returns binary text data as Python strings rather than bytearrays when using version 8.0.24 or higher of `mysql-connector-python`.
* Spark session timeouts now run properly, rather than failing because `spark.stop_session` is called without a required argument. This fixes a bug introduced in version 1.1.

# 1.3.1 (6 January 2022)
* The recommended install and update command now includes the `--ignore-installed` flag to avoid an error caused by Pip attempting the uninstall the version the package in the read-only `anaconda-wmf` base environment.

# 1.3 (21 December 2021)
* A new integration test script (`wmfdata_tests/tests.py`) makes it easy to verify that this package and the analytics infrastructure function together to allow users to access data.
* Spark session timeouts now run on a [daemon thread](https://docs.python.org/3.7/library/threading.html#threading.Thread.daemon) so that scripts using the `spark` module can exit correctly.
* `spark.run` no longer produces an error when an SQL command without output is run.
* The documentation now provides detailed instructions for setting up the package in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) given the [new Conda setup](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Anaconda) on the analytics clients.

# 1.2 (11 March 2021)
* With the help of a new `conda` module, the `spark` module has been revised to take advantage of the capabilities of the new conda-based Jupyter setup currently in development. Most significantly, the module now supports shipping a local conda environment to the Spark workers if custom dependencies are needed.
* The on-import update check now times out after 1 second to prevent long waits when the web request fails to complete (as when the `https_proxy` environment variable is not set).
* `hive.load_csv` now properly defines the file format of the new table. The previous behavior started to cause errors after Hive's default file format was changed from text to Parquet.

# 1.1 (23 February 2021)
* The new `presto` module supports querying the Data Lake using [Presto](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Presto).
* The `spark` module has been refactored to support local and custom sessions.
* A new `utils.get_dblist` function provides easy access to wiki database lists, which is particularly useful with `mariadb.run`.
* The `hive.run_cli` function now creates its temp files in a standard location, to avoid creating distracting new entries in the current working directory.

# 1.0.4 (22 July 2020)
* The code and documentation now reflect the repository's new location ([github.com/wikimedia/wmfdata-python](https://github.com/wikimedia/wmfdata-python)).
* The repository now contains a pointer to the [code of conduct for Wikimedia technical spaces](https://www.mediawiki.org/wiki/Code_of_conduct).

# 1.0.3 (29 May 2020)
* The new version notice that display on import now shows the correct URL for updating.

# 1.0.2 (19 May 2020)
* The MariaDB module now looks in the appropriate place for the database credentials based on the user's access group.

# 1.0.1 (13 March 2020)
* The minimum required pandas version has been set to 0.20.1, which introduced the `errors` module (the version preinstalled on the Analytics Clients is 0.19.2).
* The submodules are now imported in a different order to avoid dependency errors.

# 1.0.0 (13 March 2020)
## Hive
- You can now run SQL using Hive's command line interface with `hive.run_cli`.
- The `hive.run` function is now a thin wrapper around `hive.run_cli`.
  - **Breaking change**: the `spark_master`, `spark_settings`, and `app_name` parameters of `hive.run` have been removed, since the function no longer uses Spark.
  - **Breaking change**: The `fmt` paramater of `hive.run` has been renamed to `format` for consistency with the other `run` functions.
- `hive.load_csv` now drops and recreates the destination table if necessary, so the passed field specification is always used.

## Spark
- Spark SQL functionality has been moved from `hive.run` to `spark.run` so that Spark-specific and Hive CLI-specific settings parameters are not mixed in one function.
- `spark.get_session` and `spark.run` now use a preconfigured "regular" session by default, with the ability to switch to a preconfigured "large" session or override individual settings if necessary.

## MariaDB
- **Breaking change**: the deprecated `mariadb.multirun` function has been removed. Pass a list of databases to `mariadb.run` instead.
- **Breaking change**: In `mariadb.run`, the format name `"tuples"` has been removed, leaving only its synonym `"raw"` for greater consistency with the other `run` functions.
- The `mariadb` module is now available after `import wmfdata`.

## Other
- The on-import check for newer versions of the package has been improved.
  - A version message is only shown if a newer version is available.
  - The check emits a warning instead of raising an error if it fails, making it more robust to future changes in file layout or code hosting.
- `charting.set_mpl_style` no longer causes an import error.
- Many other programming, code documentation, and code style improvements.

## Known limitations
- If you call `spark.run` or `spark.get_session` while a Spark session is already running, passing different settings will have no effect. You must restart the kernel before selecting different settings.
