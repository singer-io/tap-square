import singer
from .client import SquareClient
from .streams import STREAMS

LOGGER = singer.get_logger()

def sync(config, state, catalog):
    client = SquareClient(config)

    for stream in catalog.get_selected_streams(state):
        tap_stream_id = stream.tap_stream_id
        stream_obj = STREAMS[tap_stream_id]()
        replication_key = stream_obj.replication_key
        stream_schema = stream.schema.to_dict()

        LOGGER.info('Staring sync for stream: %s', tap_stream_id)

        state = singer.set_currently_syncing(state, tap_stream_id)
        singer.write_state(state)

        singer.write_schema(
            tap_stream_id,
            stream_schema,
            stream.key_properties,
            stream.replication_key
        )

        start_time = singer.get_bookmark(state, tap_stream_id, replication_key, config['start_date'])
        bookmarked_cursor = singer.get_bookmark(state, tap_stream_id, 'cursor')

        state = stream_obj.sync(client, state, start_time, bookmarked_cursor)

        singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
