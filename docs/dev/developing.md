## Installing in development mode
When you work on changes to this package, you will want an easily-accessible local copy that's at the top of your Python path. Here's the recommended way to do that:
* Clone this repository to whatever location you like.
* Install the cloned repository in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) using `pip install -e`.
  * This will end early with the message `ERROR: Could not install packages due to an OSError: [Errno 30] Read-only file system: 'WHEEL'`. This is okay; the crucial step has already been done.
* Change to the `site-packages` directory for your current environment using the following command: `` cd `python -c "import sysconfig; print(sysconfig.get_path('purelib'))"` ``
* Edit the `anaconda.pth` file in that directory to include the absolute path (not using `~`) to the cloned repository.
  * This ensures the new version of Wmfdata is on your Python path (which `pip install -e` would have done if not for the error) _and_ ensures it takes precendence over the version of Wmfdata in the base `anaconda-wmf` environment.
  * `anaconda.pth` is not a standard part of Python or Anaconda; it's created by our custom `conda-create-stacked` utility.
