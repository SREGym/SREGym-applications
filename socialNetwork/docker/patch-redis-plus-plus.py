"""Add the accessor used by HomeTimelineService to redis-plus-plus 1.2.3."""

import sys
from pathlib import Path

header = Path(sys.argv[1])
source = header.read_text()
accessor = "ShardsPool* get_shards_pool()"
if accessor not in source:
    anchor = "    Transaction transaction"
    if source.count(anchor) != 1:
        raise SystemExit("Cannot find the expected redis-plus-plus 1.2.3 patch location")
    source = source.replace(anchor, "    " + accessor + " { return &_pool; }\n\n" + anchor)
    header.write_text(source)
