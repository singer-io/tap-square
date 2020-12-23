from datetime import timedelta
import singer
from methodtools import lru_cache
from requests.exceptions import RequestException

LOGGER = singer.get_logger()

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_date_windows(start_time):
    window_start = singer.utils.strptime_to_utc(start_time)
    now = singer.utils.now()
    while window_start < now:
        window_end = window_start + timedelta(days=7)
        if window_end > now:
            window_end = now
        yield singer.utils.strftime(window_start), singer.utils.strftime(window_end)
        window_start = window_end


class Stream:
    def __init__(self, client):
        self.client = client


class CatalogStream(Stream):
    object_type = None
    tap_stream_id = None
    replication_key = None

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time
        for page, _ in self.client.get_catalog(self.object_type, start_time):
            for record in page:
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(
                    self.tap_stream_id,
                    transformed_record,
                )
                if record[self.replication_key] > max_record_value:
                    max_record_value = transformed_record[self.replication_key]

            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
            singer.write_state(state)
        return state


class FullTableStream(Stream):
    tap_stream_id = None
    key_properties = []
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages_safe(self, state, bookmarked_cursor, start_time):
        try:
            yield from self.get_pages(bookmarked_cursor, start_time)
        except (RuntimeError, RequestException):
            # NB> If we get a non-retryable error we should delete the
            # pagination cursor bookmark before re-raising the exception.
            LOGGER.fatal("Received fatal exception during syncing of stream %s, Clearing cursor bookmark.", self.tap_stream_id)

            state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')
            singer.write_state(state)
            raise

    def get_pages(self, bookmarked_cursor, start_time):
        raise NotImplementedError("Child classes of FullTableStreams require `get_pages` implementation")

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        bookmarked_cursor = singer.get_bookmark(state, self.tap_stream_id, 'cursor')

        for page, cursor in self.get_pages_safe(state, bookmarked_cursor, start_time):
            for record in page:
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(
                    self.tap_stream_id,
                    transformed_record,
                )
            singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')
        singer.write_state(state)
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


class Employees(FullTableStream):
    tap_stream_id = 'employees'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_employees(bookmarked_cursor)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = config['start_date']
        bookmarked_cursor = singer.get_bookmark(state, self.tap_stream_id, 'cursor')

        for page, cursor in self.get_pages_safe(state, bookmarked_cursor, start_time):
            for record in page:
                if record['updated_at'] >= start_time:
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    singer.write_record(
                        self.tap_stream_id,
                        transformed_record,
                    )
            singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')
        singer.write_state(state)
        return state


class ModifierLists(CatalogStream):
    tap_stream_id = 'modifier_lists'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'MODIFIER_LIST'


class Locations(FullTableStream):
    tap_stream_id = 'locations'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    @lru_cache()
    @classmethod
    def get_all_location_ids(cls, client):
        all_location_ids = list()
        for page, _ in client.get_locations():
            for location in page:
                all_location_ids.append(location['id'])

        return all_location_ids

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_locations()


class BankAccounts(FullTableStream):
    tap_stream_id = 'bank_accounts'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'BANK ACCOUNTS'

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_bank_accounts()


class Refunds(FullTableStream):
    tap_stream_id = 'refunds'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'REFUND'

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_refunds(start_time, bookmarked_cursor)


class Payments(FullTableStream):
    tap_stream_id = 'payments'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None
    object_type = 'PAYMENT'

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_payments(start_time, bookmarked_cursor)


class Orders(Stream):
    tap_stream_id = 'orders'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'
    object_type = 'ORDER'

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time
        all_location_ids = Locations.get_all_location_ids(self.client)

        for location_ids_chunk in chunks(all_location_ids, 10):
            # orders requests can only take up to 10 location_ids at a time
            for page, _ in self.client.get_orders(location_ids_chunk, start_time):
                for record in page:
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    singer.write_record(
                        self.tap_stream_id,
                        transformed_record,
                    )
                    if record[self.replication_key] > max_record_value:
                        max_record_value = transformed_record[self.replication_key]

                state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
                singer.write_state(state)
        return state


class Inventories(FullTableStream):
    tap_stream_id = 'inventories'
    key_properties = []
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_inventories(start_time, bookmarked_cursor)


class Shifts(FullTableStream):
    tap_stream_id = 'shifts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_shifts(bookmarked_cursor)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])

        sync_start_bookmark = singer.get_bookmark(
            state,
            self.tap_stream_id,
            'sync_start',
            singer.utils.strftime(singer.utils.now(),
                                  format_str=singer.utils.DATETIME_PARSE)
        )
        state = singer.write_bookmark(
            state,
            self.tap_stream_id,
            'sync_start',
            sync_start_bookmark,
        )

        bookmarked_cursor = singer.get_bookmark(state, self.tap_stream_id, 'cursor')

        for page, cursor in self.get_pages_safe(state, bookmarked_cursor, start_time):
            for record in page:
                if record[self.replication_key] >= start_time:
                    transformed_record = transformer.transform(
                        record, stream_schema, stream_metadata,
                    )
                    singer.write_record(
                        self.tap_stream_id,
                        transformed_record,
                    )
            state = singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'sync_start')
        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')
        state = singer.write_bookmark(
            state,
            self.tap_stream_id,
            self.replication_key,
            sync_start_bookmark,
        )
        singer.write_state(state)
        return state

class Roles(FullTableStream):
    # Square Docs: you must use Connect V1 to manage employees and employee roles.
    tap_stream_id = 'roles'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages(self, bookmarked_cursor, start_time):
        yield from self.client.get_roles(bookmarked_cursor)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = config['start_date']
        bookmarked_cursor = singer.get_bookmark(state, self.tap_stream_id, 'cursor')
        for page, cursor in self.get_pages_safe(state, bookmarked_cursor, start_time):
            for record in page:
                if record['updated_at'] >= start_time:
                    transformed_record = transformer.transform(
                        record, stream_schema, stream_metadata,
                    )
                    singer.write_record(
                        self.tap_stream_id,
                        transformed_record,
                    )
            singer.write_bookmark(state, self.tap_stream_id, 'cursor', cursor)
            singer.write_state(state)

        state = singer.clear_bookmark(state, self.tap_stream_id, 'cursor')
        singer.write_state(state)
        return state


class CashDrawerShifts(FullTableStream):
    tap_stream_id = 'cash_drawer_shifts'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages(self, bookmarked_cursor, start_time):
        for location_id in Locations.get_all_location_ids(self.client):
            # Cash Drawer Shifts requests can only take up to 1 location_id at a time
            yield from self.client.get_cash_drawer_shifts(location_id, start_time, bookmarked_cursor)


class Settlements(FullTableStream):
    tap_stream_id = 'settlements'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    replication_key = None

    def get_pages(self, bookmarked_cursor, start_time):
        for location_id in Locations.get_all_location_ids(self.client):
            # Settlements requests can only take up to 1 location_id at a time
            yield from self.client.get_settlements(location_id, start_time, bookmarked_cursor)


class Customers(Stream):
    tap_stream_id = 'customers'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        for window_start, window_end in get_date_windows(start_time):
            LOGGER.info("Searching for customers from %s to %s", window_start, window_end)
            for page, _ in self.client.get_customers(window_start, window_end):
                for record in page:
                    transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                    singer.write_record(
                        self.tap_stream_id,
                        transformed_record,
                    )
            state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, window_end)
            singer.write_state(state)
        return state

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
    'roles': Roles,
    'shifts': Shifts,
    'cash_drawer_shifts': CashDrawerShifts,
    'settlements': Settlements,
    'customers': Customers
}
