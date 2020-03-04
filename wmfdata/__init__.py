# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, mariadb, metadata, spark, utils

welcome_message = """{0}

You can find the source for `wmfdata` at {1}"""

remote = utils.check_remote_version(metadata.version)
if remote['is_newer']:
    update_message = """You are using wmfdata {0}. A newer version is available.
You can update to {1} by running `pip install --upgrade git+{2}/wmfdata.git@release`
To see what has changed, refer to https://github.com/neilpquinn/wmfdata/CHANGELOG.md"""
    update_message = update_message.format(metadata.version, remote['version'], metadata.source)
else:
    update_message = "You are using wmfdata {0} (latest).".format(metadata.version)

welcome_message = welcome_message.format(update_message, metadata.source)
utils.print_err(welcome_message)
