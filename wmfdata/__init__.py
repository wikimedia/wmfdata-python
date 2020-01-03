__version__ = "0.1.1"
__source__ = "https://github.com/neilpquinn/wmfdata"

# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, utils  # mariadb,

welcome_message = """{0}

You can find the source for `wmfdata` at {1}"""

branch = "master"
remote = utils.check_remote_version(__version__, branch)
if remote['is_newer']:
    url_extra = "" if branch == "master" else "@{0}".format(branch)
    update_message = """You are using wmfdata version {0}. A newer version is available.
Update to version {1} via: pip install --upgrade git+{2}/wmfdata.git{3}"""
    update_message = update_message.format(__version__, remote['version'], __source__, url_extra)
else:
    update_message = "You are using version {0} of wmfdata (latest).".format(__version__)

welcome_message = welcome_message.format(update_message, __source__)
utils.print_err(welcome_message)
