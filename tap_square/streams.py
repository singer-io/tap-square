class CatalogStream:
    object_type = None
    tap_stream_id = None
    replication_key = None

    def sync(self, client, start_time, bookmarked_cursor):

        for page, cursor in client.get_catalog(self.object_type, start_time, bookmarked_cursor):
            yield page, cursor


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


class Employees():
    tap_stream_id = 'employees'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def sync(self, client, bookmarked_cursor): #pylint: disable=no-self-use

        for page, cursor in client.get_employees(bookmarked_cursor):
            yield page, cursor


class ModifierLists(CatalogStream):
    tap_stream_id = 'modifier_lists'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'MODIFIER_LIST'


class Locations():
    tap_stream_id = 'locations'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def sync(self, client, bookmarked_cursor): #pylint: disable=unused-argument,no-self-use

        for page, cursor in client.get_locations():
            yield page, cursor


class Refunds():
    tap_stream_id = 'refunds'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['created_at']
    replication_key = 'created_at'
    object_type = 'REFUND'

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        for page, cursor in client.get_refunds(client, start_time, bookmarked_cursor):
            yield page, cursor


class Payments():
    tap_stream_id = 'payments'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'DISCOUNT'

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        for page, cursor in client.get_payments(self.object_type, start_time, bookmarked_cursor):
            yield page, cursor


class Inventories:
    tap_stream_id = 'inventories'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        items = Items()
        all_variation_ids = set()
        for page, _ in items.sync(client, start_time, bookmarked_cursor):
            for item in page:
                for item_data_variation in item['item_data'].get('variations', list()):
                    all_variation_ids.add(item_data_variation['id'])
                
        for page, cursor in client.get_inventories(all_variation_ids, start_time):
            yield page, cursor
    

STREAMS = {
    'items': Items,
    'categories': Categories,
    'discounts': Discounts,
    'taxes': Taxes,
    'employees': Employees,
    'locations': Locations,
    'refunds': Refunds,
    'payments': Payments,
    'modifier_lists': ModifierLists
}
