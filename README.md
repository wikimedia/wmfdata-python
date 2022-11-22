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
Wmfdata comes preinstalled in the Conda environments used on the analytics clients.

The command to upgrade to the latest version depends on whether you are using an `anaconda-wmf` or `conda-analytics` environment. To find out which you're using, consult [the Conda page on Wikitech](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Conda).

In an `anaconda-wmf` environment, use:
```
pip install --upgrade git+https://github.com/wikimedia/wmfdata-python.git@release --ignore-installed
```

In a `conda-analytics` environment, use:
```
pip install --upgrade git+https://github.com/wikimedia/wmfdata-python.git@release
```

## Support and maintenance 
Tasks related to Wmfdata are tracked in Wikimedia Phabricator in the [Wmfdata-Python project](https://phabricator.wikimedia.org/project/profile/4627/).

The Wikimedia Foundation's [Product Analytics](https://www.mediawiki.org/wiki/Product_Analytics) and [Data Engineering](https://wikitech.wikimedia.org/wiki/Data_Engineering) teams are joint [code stewards](https://www.mediawiki.org/wiki/Code_Stewardship) of Wmfdata. Data Engineering is the ultimate steward of the data access and analytics infrastructure interface portions, while Product Analytics is ultimate steward of the analyst ergonomics portions. The current [maintainers](https://www.mediawiki.org/wiki/Developers/Maintainers) of wmfdata are [nshahquinn](https://github.com/nshahquinn), [ottomata](https://github.com/ottomata), [milimetric](https://github.com/milimetric/), [nettrom](https://github.com/nettrom/), and [xabriel](https://github.com/xabriel).

If you're a hero who would like to contribute code, we welcome [pull requests here on GitHub](/pulls).
