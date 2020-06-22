import singer

class Items:
    tap_stream_id = 'items'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def sync(self, client, state, start_time, bookmarked_cursor):
        max_updated_at = start_time

        for page, cursor in client.get_catalog_items(start_time, bookmarked_cursor):

            for item in page:
                singer.write_record(self.tap_stream_id, item)
                if item['updated_at'] > max_updated_at:
                    max_updated_at = item['updated_at']

            state = singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_updated_at)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')

        return state

class Categories:
    tap_stream_id = 'categories'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def sync(self, client, state, start_time, bookmarked_cursor):
        max_updated_at = start_time

        for page, cursor in client.get_catalog_categories(start_time, bookmarked_cursor):

            for item in page:
                singer.write_record(self.tap_stream_id, item)
                if item['updated_at'] > max_updated_at:
                    max_updated_at = item['updated_at']

            state = singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_updated_at)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')

        return state


class Discounts:
    tap_stream_id = 'discounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def sync(self, client, state, start_time, bookmarked_cursor):
        max_updated_at = start_time

        for page, cursor in client.get_catalog_discounts(start_time, bookmarked_cursor):

            for item in page:
                singer.write_record(self.tap_stream_id, item)
                if item['updated_at'] > max_updated_at:
                    max_updated_at = item['updated_at']

            state = singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_updated_at)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')

        return state


STREAMS = {
    'items': Items,
    'categories': Categories,
    'discounts': Discounts
}
