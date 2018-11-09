import sys
from math import log10, floor

from . import mariadb
import pandas as pd

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def list_wikis(groups=["all"]):
    
    if isinstance(groups, str):
        groups = [groups]
    
    if groups == ["all"]:
        groups.extend([
            'commons', 'incubator', 'foundation', 'mediawiki', 'meta', 'sources', 
            'species','wikibooks', 'wikidata', 'wikinews', 'wikipedia', 'wikiquote',
            'wikisource', 'wikiversity', 'wikivoyage', 'wiktionary'
        ])
        
        groups.remove("all")
      
    
    groups_list = ", ".join(["'" + group + "'" for group in groups])
    
    wikis = mariadb.run(
        """
        select site_global_key
        from enwiki.sites
        where site_group in
            ({groups}) 
        order by site_global_key asc
        """.format(groups = groups_list), 
        fmt = "raw"
    )
    
    return [row[0] for row in wikis]


def sig_figs(x, n_figs):
    exponent = floor(log10(abs(x)))
    round_level = -exponent + (n_figs - 1)
    return round(x, round_level)

def pct_str(x, decimals=1):
    format_str = "{:,." + str(decimals) + "f}%"
    return format_str.format(x * 100)
    
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