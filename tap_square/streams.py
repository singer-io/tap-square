import singer

class CatalogStream:
    object_type = None
    tap_stream_id = None
    replication_key = None

    def sync(self, client, state, start_time, bookmarked_cursor):
        max_updated_at = start_time

        for page, cursor in client.get_catalog(self.object_type, start_time, bookmarked_cursor):

            for record in page:
                singer.write_record(self.tap_stream_id, record)
                if record['updated_at'] > max_updated_at:
                    max_updated_at = record['updated_at']

            state = singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_updated_at)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')

        return state


class Items(CatalogStream):
    tap_stream_id = 'items'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'ITEM'


class Categories(CatalogStream):
    tap_stream_id = 'categories'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'CATEGORY'


class Discounts(CatalogStream):
    tap_stream_id = 'discounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'DISCOUNT'


class Taxes(CatalogStream):
    tap_stream_id = 'taxes'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'TAX'


STREAMS = {
    'items': Items,
    'categories': Categories,
    'discounts': Discounts,
    'taxes': Taxes,
}
