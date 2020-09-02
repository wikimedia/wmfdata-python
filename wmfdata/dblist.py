"""Parse Wikimedia dblists to resolve symbolic names like "group0"."""

import os.path

DEFAULT_DBLIST_PATH = "/srv/mediawiki-config/dblists"


def resolve_group(group_name, dblist_path=DEFAULT_DBLIST_PATH):
    path = os.path.join(dblist_path, group_name + ".dblist")
    with open(path) as list_file:
        lines = list_file.readlines()

    lines = map(str.strip, lines)
    lines = filter(lambda w: not w.startswith("#"), lines)
    lines = filter(lambda w: w not in ('labtestwiki',), lines)
    return list(lines)
