`wmfdata` is an Python package for analyzing Wikimedia data on Wikimedia's [non-public](https://wikitech.wikimedia.org/wiki/Analytics/Data_access#Production_access) [analytics clients](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Clients). It is maintained by the [Wikimedia Foundation Product Analytics team](https://www.mediawiki.org/wiki/Product_Analytics).

## Features
wmfdata's most popular feature is SQL data access. The `hive.run`, `spark.run`, `presto.run`, and `mariadb.run` functions allow you to run commands using these different query engines and receive the results as a Pandas dataframe in one line of code.

Other features include:
* Easy generation of Spark sessions using `spark.get_session` (or `spark.get_custom_session` if you want to fine-tine the settings)
* Loading CSV or TSV files into Hive using `hive.load_csv`
* Turning cryptic Kerberos-related errors into clear reminders to renew your Kerberos credentials

## Installation and upgrading
To install the latest version of wmfdata, use the following command:
```
pip install --upgrade git+https://github.com/wikimedia/wmfdata-python.git@release
```

This works whether or not you have an older version installed.

## Troubleshooting
### Importing wmfdata fails with `AttributeError: module 'matplotlib.ticker' has no attribute 'PercentFormatter'`

This happens because wmfdata requires matplotlib 2.1 or greater, but the preinstalled version on SWAP is older. wmfdata automatically upgrades matplotlib during its installation, but for some reason the new version doesn't take effect immediately. Restarting your Jupyter server should fix it (don't worry, this won't affect anyone else). This is different that restarting an individual notebook's kernel; to restart your server, follow these steps:
1. Navigate to `/hub/home` on your Jupyter server (for example, if your server is available at `localhost:8000`, go to `http://localhost:8000/hub/home`).
1. Click the big red button that says "Stop My Server".
1. Wait a while, reloading the page if necessary, until the red button has disappeared and you see a big green button that says "Start My Server".
1. Log back in, and everything should work correctly. 

## Support and bug reports
Tasks related to wmfdata are tracked in Wikimedia Phabricator in the [wmfdata-python project](https://phabricator.wikimedia.org/project/profile/4627/). 

You can also email the Product Analytics team at product-analytics AT wikimedia.org with questions or feedback.

If you're a hero who would like to contribute code, we welcome [pull requests here on GitHub](/pulls).
