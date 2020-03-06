# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, mariadb, metadata, spark, utils

try:
    remote = utils.check_remote_version(metadata.version)
    if remote['is_newer']:
        update_message = (
            "You are using wmfdata v{0}, but v{1} is available.\n\n" +
            "To update, run `pip install --upgrade git+{2}/wmfdata.git@release`.\n\n" +
            "To see the changes, refer to {2}/blob/release/CHANGELOG.md"
        ).format(metadata.version, remote['version'], metadata.source)
        utils.print_err(update_message)

# If the file with the version info is ever moved, or the code hosting changes, and so
# on, it will make all previous versions of the version check fail, so we should turn
# any errors into an understandable warning.
except:
    utils.print_err((
        "The check for a newer release of wmfdata failed to complete. Consider "
        "checking manually."
    ))
