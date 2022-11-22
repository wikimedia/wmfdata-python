from datetime import date

# Import all submodules so they are accessible after `import wmfdata`. utils must go
# first to prevent circular import issues. Other submodules can depend on utils and/or conda ONLY.
from wmfdata import utils, conda
from wmfdata import (
    hive,
    mariadb,
    metadata,
    presto,
    spark
)

# Warn about the conda-analytics migration once it has almost certainly begun
if conda.is_anaconda_wmf_env() and date.today() >= date(2022, 12, 12):
    utils.print_err(
        "You are using an anaconda-wmf environment.",
        "You must switch to a conda-analytics environment by 31 March 2023.",
        "For details, see https://wikitech.wikimedia.org/wiki/Analytics/Systems/Conda."
    )

try:
    remote = utils.check_remote_version(metadata.source, metadata.version)

    if remote['is_newer']:
        update_command = f"pip install --upgrade git+{metadata.source}.git@release"
        if conda.is_anaconda_wmf_env():
            update_command += " --ignore-installed"

        message = [
            f"You are using Wmfdata v{metadata.version}, but v{remote['version']} is available.",
            f"To update, run `{update_command}`.",
            f"To see the changes, refer to {metadata.source}/blob/release/CHANGELOG.md.",
        ]

        if remote["is_new_major_version"]:
            message.insert(1, "This is major upgrade, so breaking changes are likely!")

        utils.print_err(*message)

# If the file with the version info is ever moved, or the code hosting changes, and so
# on, it will make all previous versions of the version check fail, so we should turn
# any errors into an understandable warning.
except:
    utils.print_err((
        "The check for a newer release of Wmfdata failed to complete. Consider "
        "checking manually."
    ))
