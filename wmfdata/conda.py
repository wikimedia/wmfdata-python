import json
import os

# In case conda is not installed, we still want these functions to return something useful.
try:
    from conda.cli.python_api import run_command as condacli
    conda_installed = True
except ImportError:
    conda_installed = False

"""
Default kwargs to pass to conda_pack.pack.
"""
conda_pack_defaults = {
    # This is required to pack stacked conda envs.
    "ignore_editable_packages": True,
    "verbose": True,
    "force": False,
    "format": "tgz",
}

def info():
    """
    Returns the result of conda info --all --json as a dict.
    """
    if conda_installed:
        return json.loads(condacli("info", ["--all", "--json"])[0])
    else:
        return {}

def env_vars():
    """
    Returns dict of conda environment variables.
    """
    return info().get("env_vars", {})

def active_prefix():
    """
    Returns the path to the active conda env, or None.
    """
    return info().get("active_prefix", None)

def active_name():
    """
    Returns the name of the active conda env, or None.
    """
    return env_vars().get("CONDA_DEFAULT_ENV", None)

def base_prefix():
    """
    If the active conda env is stacked on another conda env, returns
    the path to that base conda env, or None.
    """
    return env_vars().get("CONDA_PREFIX_1", None)

def is_active():
    """
    Returns True if a conda env is active, else False.
    """
    if active_prefix() is None:
        return False
    else:
        return True

def is_stacked():
    """
    Returns True if a conda env active and stacked on a base conda env, else False.
    """
    if base_prefix() is None:
        return False
    else:
        return True


def is_anaconda_wmf_env():
    """
    Returns True if we detect that the conda environment is the deprecated anaconda-wmf.
    False otherwise.
    """
    conda_location = info().get('conda_location', '')
    return conda_location.startswith('/usr/lib/anaconda-wmf')


def conda_base_env_prefix():
    """
    Returns the path to the conda base env, which on WMF servers can be either
    '/usr/lib/anaconda-wmf' or '/opt/conda-analytics'.
    This can be overridden by setting the CONDA_BASE_ENV_PREFIX env var.
    """
    if is_anaconda_wmf_env():
        conda_base_env_prefix_default = '/usr/lib/anaconda-wmf'
    else:
        conda_base_env_prefix_default = '/opt/conda-analytics'

    return os.environ.get("CONDA_BASE_ENV_PREFIX", conda_base_env_prefix_default)


def pack(**conda_pack_kwargs):
    """
    Calls conda_pack.pack.
    If the packed output file already exists, this will not repackage
    it unless conda_pack_kwargs["force"] == True.

    Returns the path to the output packed env file.

    Arguments:
    * `conda_pack_kwargs` args to pass to conda_pack.pack().
    """
    kwargs = conda_pack_defaults.copy()
    kwargs.update(conda_pack_kwargs)

    # Make sure output is set to something, so we can return it if it already exists.
    if "output" not in kwargs:
        conda_env_name = "env"
        if "prefix" in kwargs:
            conda_env_name = os.path.basename(kwargs["prefix"])
        elif "name" in kwargs:
            conda_env_name = kwargs["name"]
        elif is_active():
            conda_env_name = active_name()

        kwargs["output"] = "conda-{}.{}".format(conda_env_name, kwargs["format"])

    conda_packed_file = kwargs["output"]
    if os.path.isfile(conda_packed_file) and not kwargs["force"]:
        print(
            "The requested conda environment has already been packed.\n"
            "If you want it to be repacked, set force=True in conda_pack_kwargs."
        )
        return conda_packed_file
    else:
        # Isolate the import here so that we don"t get import errors
        # if conda_pack is not installed (e.g. in a virtualenv).
        import conda_pack
        # NOTE: If no conda env is currently active, and kwargs
        # doesn"t contain information about what env to pack (i.e. no name or prefix)
        # then this raise an error.
        return conda_pack.pack(**kwargs)
