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

    def get_all_variation_ids(self, client, start_time, bookmarked_cursor):
        for page, _ in self.sync(client, start_time, bookmarked_cursor):
            for item in page:
                for item_data_variation in item['item_data'].get('variations', list()):
                    yield item_data_variation['id']



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

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use,unused-argument

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

    def _chunks(self, lst, n): #pylint: disable=no-self-use
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_all_location_ids(self, client, start_time, bookmarked_cursor, chunk_size=10):
        all_location_ids = list()
        for page, _ in self.sync(client, start_time, bookmarked_cursor):
            for location in page:
                all_location_ids.append(location['id'])

        yield from self._chunks(all_location_ids, chunk_size)

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=unused-argument,no-self-use
        for page, cursor in client.get_locations():
            yield page, cursor

class BankAccounts():
    tap_stream_id = 'bank_accounts'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'BANK ACCOUNTS'

    def sync(self, client, bookmarked_cursor): #pylint: disable=unused-argument,no-self-use
        for page, cursor in client.get_bank_accounts():
            yield page, cursor

class Refunds():
    tap_stream_id = 'refunds'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'REFUND'

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        for page, cursor in client.get_refunds(start_time, bookmarked_cursor):
            yield page, cursor


class Payments():
    tap_stream_id = 'payments'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'PAYMENT'

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        for page, cursor in client.get_payments(start_time, bookmarked_cursor):
            yield page, cursor


class Orders():
    tap_stream_id = 'orders'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'ORDER'

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        locations = Locations()

        for location_ids_chunk in locations.get_all_location_ids(client, start_time, bookmarked_cursor, chunk_size=10):
            # orders requests can only take up to 10 location_ids at a time
            for page, cursor in client.get_orders(location_ids_chunk, start_time, bookmarked_cursor):
                yield page, cursor


class Inventories:
    tap_stream_id = 'inventories'
    key_properties = []
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def sync(self, client, start_time, bookmarked_cursor): #pylint: disable=no-self-use
        for page, cursor in client.get_inventories(start_time, bookmarked_cursor):
            yield page, cursor


STREAMS = {
    'items': Items,
    'categories': Categories,
    'discounts': Discounts,
    'taxes': Taxes,
    'employees': Employees,
    'locations': Locations,
    'bank_accounts': BankAccounts,
    'refunds': Refunds,
    'payments': Payments,
    'modifier_lists': ModifierLists,
    'inventories': Inventories,
    'orders': Orders,
}
