import sys
from math import log10, floor
import re

import pandas as pd

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
    sigfigified = sig_figs(x, n_figs)
    return "{:,}".format(sigfigified)
    
def pd_display_all(df):
    with pd.option_context(
        "display.max_rows", None, 
        "display.max_columns", None,
        "display.max_colwidth", -1,
    ):
        display(df)
    
def mediawiki_dt(dt):
    """
    Converts a Python datetime.datetime object to the string datetime form
    used in MediaWiki databases.
    """
    return dt.strftime("%Y%m%d%H%M%S")

def df_to_remarkup(df):
    """
    Prints a Pandas dataframe as a Remarkup table suitable for pasting into Phabricator.
    
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
    
    # Add a pipe to the start of every line, before adding the header separator so it doesn't get a double first pipe
    remarkup_table = re.sub(r"^([^|])", r"| \1", psv_table, flags=re.MULTILINE)
    # Make the first row a header
    remarkup_table = remarkup_table.replace("\n", "\n" + header_sep + "\n", 1)
    
    print(remarkup_table)