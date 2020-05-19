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
