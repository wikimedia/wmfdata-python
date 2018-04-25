import time

def list_all_wikis():
    wikis = run_mariadb(
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
    
    return [row["site_global_key"] for row in wikis]

def list_wikis_by_group(*groups):
    groups_list = ", ".join(["'" + group + "'" for group in groups])
    
    wikis = run_mariadb(
        """
        select site_global_key
        from enwiki.sites
        where site_group in
            ({groups}) 
        order by site_global_key asc
        """.format(groups = groups_list), 
        fmt = "raw"
    )
    
    return [row["site_global_key"] for row in wikis]

def multiquery(*cmds, wikis = list_all_wikis()):
    result = None
    
    for wiki in wikis:
        init = time.perf_counter()
        
        part_result = run_mariadb(
            "use {db}".format(db = wiki),
            *cmds
        )
        
        if result is None:
            result = part_result
        else:
            result = pd.concat([result, part_result], ignore_index = True)
        
        elapsed = time.perf_counter() - init
        print_err("{} completed in {:0.0f} s".format(wiki, elapsed))
        
    return result