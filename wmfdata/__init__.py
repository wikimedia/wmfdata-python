__version__ = "0.1.0"
__source__ = "https://github.com/neilpquinn/wmfdata"

# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, utils  # mariadb,

welcome_message = """{0}

You can find the source for `wmfdata` at {1}"""

remote = utils.check_remote_version(__version__)
if remote['is_newer']:
    update_message = """You are using wmfdata {0}. A newer version is available.
Update to {1} via: pip install --upgrade git+{2}/wmfdata.git
To see what changed refer to https://github.com/neilpquinn/wmfdata/CHANGELOG.md"""
    update_message = update_message.format(__version__, remote['version'], __source__)
else:
    update_message = "You are using wmfdata {0} (latest).".format(__version__)

welcome_message = welcome_message.format(update_message, __source__)
utils.print_err(welcome_message)
