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
        yield singer.utils.strftime(window_start), singer.utils.strftime(window_end)
        window_start = window_end

def sync(config, state, catalog):
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

            start_time = singer.get_bookmark(state, tap_stream_id, replication_key, config['start_date'])
            bookmarked_cursor = singer.get_bookmark(state, tap_stream_id, 'cursor')

            if tap_stream_id == 'shifts':
                replication_key = stream_obj.replication_key

                sync_start_bookmark = singer.get_bookmark(
                    state,
                    tap_stream_id,
                    'sync_start',
                    singer.utils.strftime(singer.utils.now(),
                                          format_str=singer.utils.DATETIME_PARSE)
                )
                state = singer.write_bookmark(
                    state,
                    tap_stream_id,
                    'sync_start',
                    sync_start_bookmark,
                )
                for page, cursor in stream_obj.sync(start_time, bookmarked_cursor):
                    for record in page:
                        if record[replication_key] >= start_time:
                            transformed_record = transformer.transform(
                                record, stream_schema, stream_metadata,
                            )
                            singer.write_record(
                                tap_stream_id,
                                transformed_record,
                            )
                    state = singer.write_bookmark(state, tap_stream_id, 'cursor', cursor)
                    singer.write_state(state)

                state = singer.clear_bookmark(state, tap_stream_id, 'sync_start')
                state = singer.clear_bookmark(state, tap_stream_id, 'cursor')
                state = singer.write_bookmark(
                    state,
                    tap_stream_id,
                    replication_key,
                    sync_start_bookmark,
                )
                singer.write_state(state)

            elif tap_stream_id == 'customers':
                replication_key = stream_obj.replication_key
                cursor = None
                for window_start, window_end in get_date_windows(start_time):
                    LOGGER.info("Searching for customers from %s to %s", window_start, window_end)
                    for page, cursor in stream_obj.sync(window_start, window_end, cursor):
                        for record in page:
                            transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                            singer.write_record(
                                tap_stream_id,
                                transformed_record,
                            )

                    state = singer.write_bookmark(state, tap_stream_id, replication_key, window_end)
                    singer.write_state(state)


            elif stream_obj.replication_method == 'INCREMENTAL':
                replication_key = stream_obj.replication_key
                max_record_value = start_time
                for page, cursor in stream_obj.sync(start_time, bookmarked_cursor):
                    for record in page:
                        transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                        singer.write_record(
                            tap_stream_id,
                            transformed_record,
                        )
                        if record[replication_key] > max_record_value:
                            max_record_value = transformed_record[replication_key]

                    state = singer.write_bookmark(state, tap_stream_id, 'cursor', cursor)
                    singer.write_state(state)

                state = singer.clear_bookmark(state, tap_stream_id, 'cursor')
                state = singer.write_bookmark(state, tap_stream_id, replication_key, max_record_value)
                singer.write_state(state)
            else:
                for record in stream_obj.sync(start_time, bookmarked_cursor):
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    singer.write_record(
                        tap_stream_id,
                        transformed_record,
                    )

                state = singer.clear_bookmark(state, tap_stream_id, 'cursor')
                singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
