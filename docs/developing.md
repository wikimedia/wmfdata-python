This document has useful instructions for people who are interested in fixing bugs and adding features in Wmfdata-Python. If you just want to _use_ this package, you won't need to do these things!

## Installing in development mode
When you work on changes to this package, you will want an easily-accessible local copy that's at the top of your Python path. Here's the recommended way to do that:
1. Clone this repository to whatever location you like.
1. Install the cloned repository in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) using `pip install -e .`
    * This will end early with the message `ERROR: Could not install packages due to an OSError: [Errno 30] Read-only file system: 'WHEEL'`. This is okay; the crucial step has already been done.
    * NOTE: In some cases, this can also give you `ERROR: Could not install packages due to an OSError: [Errno 13] Permission denied...`.  In this case, you *should* follow the advice and pass `--user` as in `pip install --user -e .`
1. Change to the `site-packages` directory for your current environment using the following command: `` cd `python -c "import sysconfig; print(sysconfig.get_path('purelib'))"` ``
1. Edit the `anaconda.pth` file in that directory to include the absolute path (not using `~`) to the cloned repository.
    * This ensures the new version of Wmfdata is on your Python path (which `pip install -e` would have done if not for the error) _and_ ensures it takes precendence over the version of Wmfdata in the base `anaconda-wmf` environment.
    * `anaconda.pth` is not a standard part of Python or Anaconda; it's created by our custom `conda-create-stacked` utility.

## Releasing a new version
1. Make sure you have origin-tracking versions of the `main` and `release` branches on your computer.
    * To check, run `git remote show origin`. The list under `Local refs configured for 'git push':` should include both `main` and `release`.
    * If you're missing one, use `git checkout --track origin/{{branch}}`. Delete your local version first if necessary (`git branch -d {{branch}}`)
3. Check out the `main` branch and make sure all the changes you want to release have been merged in.
4. Run the test script (`wmfdata_tests/tests.py`) to verify that all the core functionality works properly.
5. Decide the new version number. This package follows [Semantic Versioning](https://semver.org/): 
    > Given a version number MAJOR.MINOR.PATCH, increment the: MAJOR version when you make incompatible API changes, MINOR version when you add functionality in a backwards compatible manner, and PATCH version when you make backwards compatible bug fixes.
6. Update the version number in `wmfdata/metadata.py`.
7. Update `CHANGELOG.md` with all of the noteworthy changes in the release.
8. Commit your changes using the commit message "Make version X.Y.Z".
9. Tag the commit you just made with the version (`git tag -a vX.Y.Z -m "version X.Y.Z"`).
10. Push the new commit and tag to the origin (`git push --follow-tags`). 
11. Check out the `release` branch and rebase it onto `main`. 
12. Now push this branch to the origin. This is the step that will trigger update notification to users.
14. If the release is significant, announce it to `analytics-announce@lists.wikimedia.org`.
