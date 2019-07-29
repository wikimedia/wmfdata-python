* Change `run()` functions to:
    * accept file paths as arguments as well as strings
    * save output directly to a data file, without the intermediate Pandas dataframe step
    * automatically log generation datetime and calculation duration
* Update `hive.run()` to:
    * Include column names in tuple format
    * Accept `date_col` and `index_col` parameters
* Add a utility for fetching lists of wikis (e.g. for passing multiple databases to `mariadb.run()`)
* Add a utility to convert between various date representations (MediaWiki, Hive, etc)
* Add a function that quickly adds a button to toggle off the raw code (along [these lines](https://chris-said.io/2016/02/13/how-to-make-polished-jupyter-presentations-with-optional-code-visibility/))
