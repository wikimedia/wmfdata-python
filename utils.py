import sys

from . import mariadb

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
