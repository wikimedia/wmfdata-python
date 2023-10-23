import logging
from math import log10, floor
import os.path
import re
import subprocess
import sys

from IPython.display import HTML
from packaging.version import Version
import pandas as pd
import requests

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, sep="\n\n", **kwargs)

    # In Jupyter notebooks, std_err messages are highlighted in red. This puts a non-highlighted
    # line afterward to more clearly separate this from other messages.
    print()

def sig_figs(x, n_figs):
    exponent = floor(log10(abs(x)))
    round_level = -exponent + (n_figs - 1)
    return round(x, round_level)

def pct_str(x, decimals=1):
    format_str = "{:,." + str(decimals) + "f}%"
    return format_str.format(x * 100)

def num_str(x, n_figs=2):
    try:
        sigfigified = sig_figs(x, n_figs)
        return "{:,}".format(sigfigified)
    except (ValueError, TypeError): # Catch numpy.NaNs and Nones
        return None

def pd_display_all(df):
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.max_colwidth", None
    ):
        display(df)

def insert_code_toggle():
    """
    Outputs a button that will show or hide the code cells in exported HTML
    versions of the notebook.
    """

    # Based on a StackOverflow answer by harshil
    # https://stackoverflow.com/a/28073228/2509972
    display(HTML("""
    <form action="javascript:code_toggle()">
        <input
          id="code_toggle"
          type="submit"
          value="Hide code"
          style="font-size: 1.4em"
        >
    </form>

    <script>
    code_shown = true;

    function code_toggle() {
        if (code_shown) {
            $('div.input, div.output_prompt').hide();
            $('#code_toggle').attr("value", "Show code");

        } else {
            $('div.input, div.output_pr').show();
            $('#code_toggle').attr("value", "Hide code");
        }

        code_shown = !code_shown
    }

    $(document).ready(code_toggle);
    </script>
    """))

def mediawiki_dt(dt):
    """
    Converts a Python datetime.datetime object to the string datetime form
    used in MediaWiki databases.
    """
    return dt.strftime("%Y%m%d%H%M%S")

def df_to_remarkup(df, **kwargs):
    """
    Prints a Pandas dataframe as a Remarkup table suitable for pasting into Phabricator.

    Note that among many kwargs the following are useful for editing the output:
        index (bool): passing `False` removes the dataframe index (default is `True`)
            Ex: `df_to_remarkup(df, index=False)`

        floatfmt (str or list(str)): the decimal place to round the outputs to
            Ex: `df_to_remarkup(df, floatfmt=(".0f", ".1f"))` (round off first column and to first decimal in second)

    See the options for pandas.DataFrame.to_markdown and the Python package tabulate for other kwargs.
    """
    print(df.to_markdown(tablefmt="pipe", **kwargs).replace(":", "-"))

def check_remote_version(source_url, local_version):
    r = requests.get(source_url + "/raw/release/wmfdata/metadata.py", timeout=1)
    # Raise an error if the page couldn't be loaded
    r.raise_for_status()

    remote_version = re.search('(([0-9]+\\.?){2,3})', r.text).group()
    remote_version = Version(remote_version)
    local_version = Version(local_version)

    d = {
        'version': str(remote_version),
        'is_newer': remote_version > local_version,
        'is_new_major_version': remote_version.major > local_version.major
    }

    return d

def check_kerberos_auth():
    klist = subprocess.call(["klist", "-s"])
    if klist == 1:
        raise OSError(
            "You do not have Kerberos credentials. Authenticate using `kinit` "
            "or run your script as a keytab-enabled user."
        )
    elif klist != 0:
        raise OSError(
          "There was an unknown issue checking your Kerberos credentials."
        )

def ensure_list(str_or_list):
    """
    Given a string, wraps it in a list; given a list, returns it unchanged.

    Useful for allowing a function to take a string for a single item or a list
    of strings for multiple items.
    """
    if isinstance(str_or_list, str):
        return [str_or_list]
    else:
        return str_or_list

def get_dblist(dblist_name, dblist_path="/srv/mediawiki-config/dblists"):
    """
    Given the name of a dblist (e.g. "wikipedia", "closed", "group0"), return the wiki database names in that list.

    To see all the dblists, visit:
    https://github.com/wikimedia/operations-mediawiki-config/tree/master/dblists
    """

    path = os.path.join(dblist_path, dblist_name + ".dblist")
    with open(path) as list_file:
        lines = list_file.readlines()

    lines = map(str.strip, lines)
    lines = filter(lambda w: not w.startswith("#"), lines)
    return list(lines)

def python_version():
    """
    Returns currently running python major.minor version. E.g "3.7"
    """
    return f"{sys.version_info.major}.{sys.version_info.minor}"

def sql_tuple(i):
    """
    Given a Python iterable, returns a string representation that can be used in an SQL IN
    clause.

    For example:
    > sql_tuple(["a", "b", "c"])
    "('a', 'b', 'c')"

    WARNING: In some cases, this function produces incorrect results with strings that contain
    single quotes or backslashes. If you encounter this situation, consult the code comments or ask
    the maintainers for help.
    """

    # Transform other iterables into lists, raising errors for non-iterables
    if type(i) != list:
        i = [x for x in i]

    # It might seem useful to return "()" when an empty iterable is passed, but "IN ()"
    # results in an SQL syntax error. Instead, we should send a clear signal to the caller
    # that it needs to better handle the no-items case.
    #
    # Running this check after converting i to a list avoids errors from generators that
    # don't implement len.
    if len(i) == 0:
        raise ValueError("Cannot produce an SQL tuple without any items.")

    # Using Python's string representation functionality means we get escaping for free. Using the
    # representation of a tuple almost works, but fails when there's just one element, because SQL
    # doesn't accept the trailing comma that Python uses. Instead, we use representation of a
    # list and replace the brackets with parentheses.
    #
    # To-do: The string representation method sometimes fails when an element contains
    # a single quote or a backslash because the escaping it produces may not match the escaping
    # expected by the query engine. For example, with a single quote, this method will escape it
    # by wrapping the element with double quotes rather than single quotes. This works for Spark
    # and MariaDB, but Presto strictly follows the ANSI SQL standard, so it interprets
    # double-quoted strings as literal column names rather than strings.
    #
    # Even worse, there is no single escaping scheme that works for all of MariaDB, Spark, and
    # Presto. To produce correct behavior in all cases, it will probably be necessary to allow the
    # user to specify the query engine and produce different results accordingly. For more
    # information, see:
    # https://github.com/nshahquinn/misc-wikimedia-analysis/blob/master
    # /2022-10_SQL_string_escaping.ipynb
    list_repr = repr(i)
    return "(" + list_repr[1:-1] + ")"
