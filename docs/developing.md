This document has useful instructions for people who are interested fixing bugs and adding features in Wmfdata-Python. If you just want to _use_ this package, you won't need to do these things!

## Installing in development mode
When you work on changes to this package, you will want an easily-accessible local copy that's at the top of your Python path. Here's the recommended way to do that:
1. Clone this repository to whatever location you like.
1. Install the cloned repository in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) using `pip install -e`.
    * This will end early with the message `ERROR: Could not install packages due to an OSError: [Errno 30] Read-only file system: 'WHEEL'`. This is okay; the crucial step has already been done.
1. Change to the `site-packages` directory for your current environment using the following command: `` cd `python -c "import sysconfig; print(sysconfig.get_path('purelib'))"` ``
1. Edit the `anaconda.pth` file in that directory to include the absolute path (not using `~`) to the cloned repository.
    * This ensures the new version of Wmfdata is on your Python path (which `pip install -e` would have done if not for the error) _and_ ensures it takes precendence over the version of Wmfdata in the base `anaconda-wmf` environment.
    * `anaconda.pth` is not a standard part of Python or Anaconda; it's created by our custom `conda-create-stacked` utility.

## Releasing a new version
1. Ensure that you are on the `master` branch and all the changes you want to include have been merged in.
1. Test that all the core functionality works properly using the test script (`wmfdata_tests/tests.py`). 
1. Decide the new version number. This package follows [Semantic Versioning](https://semver.org/): "given a version number MAJOR.MINOR.PATCH, increment the: MAJOR version when you make incompatible API changes, MINOR version when you add functionality in a backwards compatible manner, and PATCH version when you make backwards compatible bug fixes."
1. Update the version number in `wmfdata/metadata.py`.
1. Update `CHANGELOG.md` with all of the noteworthy changes in the release.
1. Commit your changes using the commit message "Make version X.Y.Z".
1. Tag the commit you just made with the version (`git tag vX.Y.Z`).
1. Push your new tag to the remote (`git push --tags`).
1. Checkout the `release` branch and rebase it onto `master`. This is the step that will trigger update notification to users.
1. If the release is significant, announce it to `analytics-announce@lists.wikimedia.org`.
