# Import all submodules so all are accessible after `import wmfdata`
from wmfdata import charting, hive, mariadb, metadata, spark, utils

remote = utils.check_remote_version(metadata.version)
if remote['is_newer']:
    update_message = (
        "You are using wmfdata v{0}, but v{1} is available.\n\n" +
        "To update, run `pip install --upgrade git+{2}/wmfdata.git@release`.\n\n" +
        "To see the changes, refer to {2}/blob/master/CHANGELOG.md"
    ).format(metadata.version, remote['version'], metadata.source)
    utils.print_err(update_message)
