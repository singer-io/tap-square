#!/usr/bin/env python3

import datetime
import json
from singer import metadata
import sys
import os
import requests
import singer

LOGGER = singer.get_logger()

STREAMS = {
    'items': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['updated_at']
    }
}

def get_abs_path(path):
    """ returns the file path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():

    """ Reads the schemas for all the apis and returns stream sets """

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        
        meta = metadata.get_standard_metadata(
            schema=schema,
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata

def discover():

    schemas, schemas_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta
        }
        
        streams.append(catalog_entry)

    return {'streams': streams}

@singer.utils.handle_top_exception(LOGGER)
def main():

    args = singer.utils.parse_args([])

    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    else:

        if args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = discover()

        state = args.state or {'bookmarks': {}}

        # sync(args.config, state, catalog)


if __name__ == '__main__':
    main()
