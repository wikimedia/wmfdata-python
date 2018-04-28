import sys

from . import mariadb

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    
def list_all_wikis():
    wikis = mariadb.run(
        """
        select site_global_key
        from enwiki.sites
        where site_group in
            ('commons', 'incubator', 'foundation', 'mediawiki', 'meta', 'sources', 
            'species','wikibooks', 'wikidata', 'wikinews', 'wikipedia', 'wikiquote',
            'wikisource', 'wikiversity', 'wikivoyage', 'wiktionary') 
        order by site_global_key asc
        """, 
        fmt = "raw"
    )
    
    return [row[0] for row in wikis]

def list_wikis_by_group(*groups):
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
