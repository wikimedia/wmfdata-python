* Change `run()` functions to:
    * accept file paths as arguments as well as strings
    * save output directly to a data file, without the intermediate Pandas dataframe step
    * automatically log generation datetime and calculation duration
* Update `hive.run()` to:
    * Include column names in tuple format
    * Accept `date_col` and `index_col` parameters
* Add a utility for fetching lists of wikis (e.g. for passing multiple databases to `mariadb.run()`)
* Add a utility to convert between various date representations (MediaWiki, Hive, etc)
* Update `hive.load_csv` to drop the table if it already exists, so the passed fieldspec is always used
