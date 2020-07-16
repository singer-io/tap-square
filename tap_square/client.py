from datetime import timedelta
from square.client import Client
from singer import utils
import singer

class SquareClient():
    def __init__(self, config):
        self._refresh_token = config['refresh_token']
        self._client_id = config['client_id']
        self._client_secret = config['client_secret']

        self._environment = 'sandbox' if config.get('sandbox') == 'true' else 'production'

        self._access_token = self._get_access_token()
        self._client = Client(access_token=self._access_token, environment=self._environment)


    def _get_access_token(self):
        body = {
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': self._refresh_token
        }

        client = Client(environment=self._environment)

        with singer.http_request_timer('GET access token'):
            result = client.o_auth.obtain_token(body)

        if result.is_error():
            raise Exception(result.errors)

        return result.body['access_token']

    def get_catalog(self, object_type, start_time, bookmarked_cursor):
        # Move the max_updated_at back the smallest unit possible
        # because the begin_time query param is exclusive
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
            "object_types": [object_type],
            "include_deleted_objects": True,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET ' + object_type):
            result = self._client.catalog.search_catalog_objects(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('objects', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET ' + object_type):
                result = self._client.catalog.search_catalog_objects(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('objects', []), result.body.get('cursor'))


    def get_employees(self, bookmarked_cursor):

        body = {
            'limit': 50,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        with singer.http_request_timer('GET employees'):
            result = self._client.employees.list_employees(**body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('employees', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET employees'):
                result = self._client.employees.list_employees(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('employees', []), result.body.get('cursor'))

    def get_locations(self):
        body = {}
        with singer.http_request_timer('GET locations'):
            result = self._client.locations.list_locations()

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('locations', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body.get('cursor')
            with singer.http_request_timer('GET locations'):
                result = self._client.locations.list_locations(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('locations', []), result.body.get('cursor'))

    def get_bank_accounts(self):
        body = {}

        with singer.http_request_timer('GET bank accounts'):
            result = self._client.bank_accounts.list_bank_accounts()

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('bank_accounts', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET bank accounts'):
                result = self._client.bank_accounts.list_bank_accounts(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('bank_accounts', []), result.body.get('cursor'))

    def get_refunds(self, object_type, start_time, bookmarked_cursor): # TODO:check sort_order input
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET refunds'):
            result = self._client.refunds.list_payment_refunds(**body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('refunds', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET ' + object_type):
                result = self._client.refunds.list_payment_refunds(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('refunds', []), result.body.get('cursor'))


    def get_payments(self, object_type, start_time, bookmarked_cursor):
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET payments'):
            result = self._client.payments.list_payments(**body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('payments', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET ' + object_type):
                result = self._client.payments.list_payments(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('payments', []), result.body.get('cursor'))
