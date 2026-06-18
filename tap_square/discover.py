import json
import os
import singer
from singer import metadata
from singer.catalog import Catalog
from .streams import STREAMS
from .client import SquareForbiddenError

LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# NB: These streams cannot be queried using Sandbox OAuth credentials
PRODUCTION_ONLY_STREAMS = {'bank_accounts', 'payouts'}

def get_schemas(sandbox):

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_object in STREAMS.items():
        if sandbox and stream_name in PRODUCTION_ONLY_STREAMS:
            continue

        schema_path = get_abs_path(f'schemas/{stream_name}.json')
        with open(schema_path, encoding='utf-8') as file:
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


def _apply_access_checks(client, schemas, schemas_metadata):
    """
    Probe each stream for read access and remove inaccessible streams from
    schemas and schemas_metadata in place.
    Raises SquareForbiddenError if no streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas
        and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        schemas_metadata.pop(stream_name, None)

    if not schemas:
        raise SquareForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' "
            "access to any supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(sorted(inaccessible_streams)),
        )


def discover(client, sandbox):
    """
    Run discovery mode and return the catalog.
    Access to each stream is verified using the provided client; streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, schemas_metadata = get_schemas(sandbox)
    _apply_access_checks(client, schemas, schemas_metadata)

    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta,
            'key_properties': STREAMS[schema_name].key_properties,
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})
