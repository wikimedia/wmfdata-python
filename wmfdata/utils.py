import logging
from math import log10, floor
import os.path
import re
import subprocess
import sys

from IPython.display import HTML
from packaging import version
import pandas as pd
import requests

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

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
        "display.max_colwidth", -1,
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

def df_to_remarkup(df):
    """
    Prints a Pandas dataframe as a Remarkup table suitable for pasting into
    Phabricator.

    Best used via the `pipe`, as in `my_dataframe.pipe(df_to_remarkup)`.
    """
    # To-do: allow printing indexes
    col_count = len(df.columns)
    header_sep = "| ----- " * col_count
    psv_table = (
        df
        .to_csv(sep="|", index=False)
        # Pad every pipe with spaces so the markup is easier to read
        .replace("|", " | ")
    )

    # Add a pipe to the start of every line, before adding the header separator
    # so it doesn't get a double first pipe
    remarkup_table = re.sub(r"^([^|])", r"| \1", psv_table, flags=re.MULTILINE)
    # Make the first row a header
    remarkup_table = remarkup_table.replace("\n", "\n" + header_sep + "\n", 1)

    print(remarkup_table)

def check_remote_version(source_url, local_version):
    r = requests.get(source_url + "/raw/release/wmfdata/metadata.py", timeout=1)
    # Raise an error if the page couldn't be loaded
    r.raise_for_status()

    remote_version = re.search('(([0-9]+\\.?){2,3})', r.text).group()

    d = {
        'version': remote_version,
        'is_newer': version.parse(remote_version) > version.parse(local_version)
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
    """
    # It might seem useful to return "()" when an empty iterable is passed, but "IN ()"
    # results in an SQL syntax error. Instead, we should send a clear signal to the caller
    # that it needs to better handle the no-items case.
    if len(i) == 0:
        raise ValueError("Cannot produce an SQL tuple without any items.")

    # Transform other iterables into lists, raising errors for non-iterables
    if type(i) != list:
        i = [x for x in i]

    # Using Python's string representation functionality means we get escaping for free. Using the
    # representation of a tuple almost works, but fails when there's just one element, because SQL
    # doesn't accept the trailing comma that Python uses. Instead, we use representation of a
    # list and replace the brackets with parentheses.
    list_repr = repr(i)
    return "(" + list_repr[1:-1] + ")"
