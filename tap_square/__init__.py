#!/usr/bin/env python3
import json

import singer
from singer.catalog import write_catalog
from tap_square.client import SquareClient
from tap_square.discover import discover
from tap_square.sync import sync

LOGGER = singer.get_logger()

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args([])

    is_sandbox = args.config.get('sandbox')
    if isinstance(is_sandbox, str):
        is_sandbox = args.config.get('sandbox') == 'true'

    client = SquareClient(args.config, args.config_path)

    if args.discover:
        catalog = discover(client, is_sandbox)
        write_catalog(catalog)
    else:
        catalog = args.catalog if args.catalog else discover(client, is_sandbox)
        sync(args.config, args.config_path, args.state, catalog)

if __name__ == '__main__':
    main()
