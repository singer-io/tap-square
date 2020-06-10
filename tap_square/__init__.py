#!/usr/bin/env python3

import singer
from .discover import discover
import json


LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args([])

    catalog = args.catalog.to_dict() if args.catalog else discover()

    if args.discover:
        print(json.dumps(catalog, indent=2))
    else:
        state = args.state or {'bookmarks': {}}
        #sync(args.config, state, catalog)


if __name__ == '__main__':
    main()
