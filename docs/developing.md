This document has useful instructions for people who are interested in fixing bugs and adding features in Wmfdata-Python. If you just want to _use_ this package, you won't need to do these things!

## Installing in development mode
When you work on changes to this package, you will want an easily-accessible local copy that's at the top of your Python path. It's easy:
1. Clone this repository to whatever location you like.
1. Install the cloned repository in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) using `pip install -e .`

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
7. If there are any new user-facing methods, add them to the [quickstart notebook](quickstart.ipynb).
7. Reinstall your local copy of Wmfdata using `pip install -e`. This ensures that the quickstart notebook will display the new version number.
7. In any case, re-run the quickstart notebook to ensure that it's fully up to date.
8. Commit your changes using the commit message "Make version X.Y.Z".
9. Tag the commit you just made with the version (`git tag -a vX.Y.Z -m "version X.Y.Z"`).
10. Push the new commit and tag to the origin (`git push --follow-tags`). 
11. Check out the `release` branch and rebase it onto `main`. 
12. Now push this branch to the origin. This is the step that will trigger update notification to users.
14. If the release is significant, announce it to `analytics-announce@lists.wikimedia.org`.
