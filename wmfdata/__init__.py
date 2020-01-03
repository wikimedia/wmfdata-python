__version__ = "0.1.0"
__source__ = "https://github.com/neilpquinn/wmfdata"

# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, utils  # mariadb,

welcome_message = "You are using wmfdata version {0}. You can find the source for `wmfdata` at {1}"
welcome_message.format(__version__, __source__)

utils.print_err(welcome_message)
