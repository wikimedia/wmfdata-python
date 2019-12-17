`wmfdata` is an Python package for analyzing Wikimedia data on the [non-public](https://wikitech.wikimedia.org/wiki/Analytics/Data_access#Production_access) [Simple Wikimedia Analytics Platform](https://wikitech.wikimedia.org/wiki/SWAP).

## Philosophy
`wmfdata` is meant to be [opinionated](https://stackoverflow.com/questions/802050/what-is-opinionated-software) in order to promote a standard workflow within the Wikimedia research and analysis community, but deciding the details of that standard has only just begun. Your feedback is welcome!

## Installation
If you have Python installed, you should be able to install the latest version of package using the following terminal command:
```
pip install git+https://github.com/neilpquinn/wmfdata.git
```

## Upgrading
If you have an older version of wmfdata and want to upgrade to the latest, simply add the `--upgrade` flag to the command, resulting in the following:
```
pip install --upgrade git+https://github.com/neilpquinn/wmfdata.git
```

## Troubleshooting
### Importing wmfdata fails with `AttributeError: module 'matplotlib.ticker' has no attribute 'PercentFormatter'`

This happens because wmfdata requires matplotlib 2.1 or greater, but the preinstalled version on SWAP is older. wmfdata automatically upgrades matplotlib during its installation, but for some reason the new version doesn't take effect immediately. Restarting your Jupyter server should fix it (don't worry, this won't affect anyone else). This is different that restarting an individual notebook's kernel; to restart your server, follow these steps:
1. Navigate to `/hub/home` on your Jupyter server (for example, if your server is available at `localhost:8000`, go to `http://localhost:8000/hub/home`).
1. Click the big red button that says "Stop My Server".
1. Wait a while, reloading the page if necessary, until the red button has disappeared and you see a big green button that says "Start My Server".
1. Log back in, and everything should work correctly. 

## Support and bug reports
If you have issues or general feedback, please contact [Neil Patel Quinn](https://meta.wikimedia.org/wiki/User:Neil_P._Quinn-WMF). If you're ready to file a specific bug report or feature request, please use [this Wikimedia Phabricator link](https://phabricator.wikimedia.org/maniphest/task/edit/form/1/?tags=product-analytics&subscribers=Neil_P._Quinn_WMF) to file a task with the tag `product-analytics` and the subscriber `Neil_P._Quinn_WMF`.

If you'd like to contribute code, you're my hero! Just make a [pull request here on GitHub](/pulls).
