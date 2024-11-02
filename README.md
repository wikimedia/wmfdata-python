`wmfdata` is an Python package for analyzing Wikimedia data on Wikimedia's [non-public](https://wikitech.wikimedia.org/wiki/Analytics/Data_access#Production_access) [analytics clients](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Clients).

## Features
Wmfdata's most popular feature is SQL data access. The `hive.run`, `spark.run`, `presto.run`, and `mariadb.run` functions allow you to run commands using these different query engines and receive the results as a Pandas dataframe, with just a single line of code.

Other features include:
* Easy generation of Spark sessions using `spark.create_session` (or `spark.create_custom_session` if you want to fine-tune the settings)
* Loading CSV or TSV files into Hive using `hive.load_csv`
* Turning cryptic Kerberos-related errors into clear reminders to renew your Kerberos credentials

## Documentation
For an introduction to using Wmfdata, see [the quickstart notebook](docs/quickstart.ipynb).

## Installation and upgrading
Wmfdata comes preinstalled in the [Conda environments used on the analytics clients](https://wikitech.wikimedia.org/wiki/Data_Engineering/Systems/Conda).

To upgrade to a newer version, use:
```
pip install --upgrade git+https://gitlab.wikimedia.org/repos/data-engineering/wmfdata-python.git@release
```

## Support and maintenance 
Tasks related to Wmfdata are tracked in Wikimedia Phabricator in the [Wmfdata-Python project](https://phabricator.wikimedia.org/project/profile/4627/). The best starting place is the [backlog in priority order](https://phabricator.wikimedia.org/maniphest/query/f9Q6SKeGTAn_/#R).

The Wikimedia Foundation's [Movement Insights](https://meta.wikimedia.org/wiki/Movement_Insights) and [Data Products](https://www.mediawiki.org/wiki/Data_Platform_Engineering/Data_Products) teams are joint [code stewards](https://www.mediawiki.org/wiki/Code_Stewardship) of Wmfdata. Data Products is the ultimate steward of the data access and analytics infrastructure interface portions, while Movement Insights is ultimate steward of the analyst ergonomics portions.

The current maintainers of Wmfdata are:
- @nshahquinn-wmf
- @xcollazo

If you're a hero who would like to contribute code, we welcome [merge requests](../../merge_requests)!
