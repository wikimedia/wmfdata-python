## Installing in development mode
When you work on changes to this package, you will want an easily-accessible local copy that's at the top of your Python path. Here's the recommended way to do that:
* Clone this repo to a convenient location like your home folder.
* Install the repo in [development mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) using `pip install -e`. 
* Find the location of your current Python installation using the command `` readlink -f `which python` ``. This should be in a Conda environment in your home folder, like `/srv/home/neilpquinn-wmf/.conda/envs/2021-06-10T22.32.37_neilpquinn-wmf/bin/python3.7`.
* Go to the `site-packages` folder in that directory.
* Find the `anaconda.pth` file, which is created by our custom `conda-create-stacked` utility.
* Edit the file to include the path to your cloned repo at the top (`pip install -e` already added the repo to the path, but this ensures that it takes precedences over the copy of wmfdata in the base Conda environment). 
