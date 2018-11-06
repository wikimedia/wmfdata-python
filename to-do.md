* Change `run()` functions to:
  * accept file paths as arguments as well as strings
  * save output directly to a data file, without the intermediate Pandas dataframe step
  * automatically log generation datetime, calculation duration, data sample, and data info
* Add a utility to convert between various date representations (MediaWiki, Hive, etc)
