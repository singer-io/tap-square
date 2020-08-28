import json
import os
from singer import metadata
from singer.catalog import Catalog
from .streams import STREAMS

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# NB: These streams cannot be queried using Sandbox OAuth credentials
PRODUCTION_ONLY_STREAMS = {'roles', 'bank_accounts', 'settlements'}

def get_schemas(sandbox):

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_object in STREAMS.items():
        if sandbox and stream_name in PRODUCTION_ONLY_STREAMS:
            continue

        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)

        meta = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_object.key_properties,
            replication_method=stream_object.replication_method
        )

        meta = metadata.to_map(meta)

        if stream_object.valid_replication_keys:
            meta = metadata.write(meta, (), 'valid-replication-keys', stream_object.valid_replication_keys)
        if stream_object.replication_key:
            meta = metadata.write(meta, ('properties', stream_object.replication_key), 'inclusion', 'automatic')

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def discover(sandbox):

    schemas, schemas_metadata = get_schemas(sandbox)
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

    return Catalog.from_dict({'streams': streams})
