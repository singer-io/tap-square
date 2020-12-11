from datetime import timedelta
import singer
from singer import Transformer, metadata
from .client import SquareClient
from .streams import STREAMS


LOGGER = singer.get_logger()

def get_date_windows(start_time):
    window_start = singer.utils.strptime_to_utc(start_time)
    now = singer.utils.now()
    while window_start < now:
        window_end = window_start + timedelta(days=7)
        if window_end > now:
            window_end = now
        yield singer.utils.strftime(window_start), singer.utils.strftime(window_end)
        window_start = window_end

def sync(config, state, catalog): # pylint: disable=too-many-statements
    client = SquareClient(config)

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, state)
            replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Staring sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(state, stream_schema, stream_metadata, config)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
