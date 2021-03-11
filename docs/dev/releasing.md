To release a new version of wmfdata-python:
1. Ensure that you are on the `master` branch and all the changes you want to include have been merged in.
1. Test that all the core functionality works properly (test script [coming soon](https://github.com/wikimedia/wmfdata-python/pull/16)). 
1. Decide the new version number. This package follows [Semantic Versioning](https://semver.org/): "given a version number MAJOR.MINOR.PATCH, increment the: MAJOR version when you make incompatible API changes, MINOR version when you add functionality in a backwards compatible manner, and PATCH version when you make backwards compatible bug fixes."
1. Update the version number in `wmfdata/metadata.py`.
1. Update `CHANGELOG.md` with all of the noteworthy changes in the release.
1. Commit your changes using the commit message "Make version X.Y.Z".
1. Tag the commit you just made with the version (`git tag vX.Y.Z`).
1. Push your new tag to the remote (`git push --tags`).
1. Checkout the `release` branch and rebase it onto `master`. This is the step that will trigger update notification to users.
1. If the release is significant, announce it to `analytics-announce@lists.wikimedia.org`.
