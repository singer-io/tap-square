from datetime import timedelta
import urllib.parse

from square.client import Client
from singer import utils
import singer
import requests

LOGGER = singer.get_logger()


def get_batch_token_from_headers(headers):
    link = headers.get('link')
    if link:
        batch_token_url = requests.utils.parse_header_links(link)[0]['url']
        parsed_link = urllib.parse.urlparse(batch_token_url)
        parsed_query = urllib.parse.parse_qs(parsed_link.query)
        return parsed_query['batch_token'][0]
    else:
        return None


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
            error_message = result.errors if result.errors else result.body
            raise Exception(error_message)

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

    def get_orders(self, location_ids, start_time, bookmarked_cursor):
        if bookmarked_cursor:
            body = {
                "cursor": bookmarked_cursor,
            }
        else:
            body = {
                "query": {
                    "filter": {
                        "date_time_filter": {
                            "updated_at": {
                                "start_at": start_time
                            }
                        }
                    },
                    "sort": {
                        "sort_field": "UPDATED_AT",
                        "sort_order": "ASC"
                    }
                }
            }

        body['location_ids'] = location_ids

        with singer.http_request_timer('GET orders'):
            result = self._client.orders.search_orders(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('orders', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            with singer.http_request_timer('GET orders'):
                body['cursor'] = result.body.get('cursor')
                result = self._client.orders.search_orders(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('orders', []), result.body.get('cursor'))


    def get_inventories(self, start_time, bookmarked_cursor):
        body = {'updated_after': start_time}

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        with singer.http_request_timer('GET inventories'):
            result = self._client.inventory.batch_retrieve_inventory_counts(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('counts', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            with singer.http_request_timer('GET inventories'):
                body['cursor'] = result.body.get('cursor')
                result = self._client.inventory.batch_retrieve_inventory_counts(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('counts', []), result.body.get('cursor'))


    # TODO: Use start_time in a later iteration, ignoring in pylint for now
    def get_shifts(self, start_time): #pylint: disable=unused-argument
        body = {
            "query": {
                "sort": {
                    "field": "UPDATED_AT",
                    "order": "ASC"
                }
            }
        }
        with singer.http_request_timer('GET shifts'):
            result = self._client.labor.search_shifts(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('shifts', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body.get('cursor')
            with singer.http_request_timer('GET shifts'):
                result = self._client.labor.search_shifts(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('shifts', []), result.body.get('cursor'))

    def get_refunds(self, start_time, bookmarked_cursor):  # TODO:check sort_order input
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
            with singer.http_request_timer('GET refunds'):
                result = self._client.refunds.list_payment_refunds(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('refunds', []), result.body.get('cursor'))


    def get_payments(self, start_time, bookmarked_cursor):
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
            with singer.http_request_timer('GET payments'):
                result = self._client.payments.list_payments(**body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('payments', []), result.body.get('cursor'))

    def get_roles(self, bookmarked_cursor):
        headers = {
            'Authorization': 'Bearer ' + self._access_token,
            'Content-Type': 'application/json'
        }
        params = {}
        url = 'https://connect.squareup.com/v1/me/roles'


        if bookmarked_cursor:
            params['batch_token'] = bookmarked_cursor

        with singer.http_request_timer('GET payments'):
            result = requests.get(url, headers=headers, params=params)

        if result.status_code != 200:
            raise Exception(result.reason)

        batch_token = get_batch_token_from_headers(result.headers)

        yield (result.json(), batch_token)

        while batch_token:
            params['batch_token'] = batch_token
            with singer.http_request_timer('GET payments'):
                result = requests.get(url, headers=headers, params=params)

            if result.status_code != 200:
                raise Exception(result.reason)

            batch_token = get_batch_token_from_headers(result.headers)

            yield (result.json(), batch_token)

    def get_cash_drawer_shifts(self, location_id, start_time, bookmarked_cursor):
        end_time = utils.strftime(utils.now(), utils.DATETIME_PARSE)
        with singer.http_request_timer('GET cash drawer shifts'):
            result = self._client.cash_drawers.list_cash_drawer_shifts(
                location_id=location_id,
                begin_time=start_time,
                end_time=end_time,
                cursor=bookmarked_cursor,
                limit=1000,
            )

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('items', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            with singer.http_request_timer('GET cash drawer shifts'):
                result = self._client.cash_drawers.list_cash_drawer_shifts(
                    location_id=location_id,
                    begin_time=start_time,
                    end_time=end_time,
                    cursor=result.body.get('cursor'),
                    limit=1000,
                )

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('items', []), result.body.get('cursor'))

    def get_settlements(self, location_id):

        url = 'https://connect.squareup.com/v1/{}/settlements'.format(location_id)
        headers = {
            'content-type': 'application/json',
            'authorization': 'Bearer {}'.format(self._access_token)
        }
        params = {
            'limit': 200,
        }
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()

        batch_token = get_batch_token_from_headers(resp.headers)

        yield (resp.json(), batch_token)

        while batch_token:
            with singer.http_request_timer('GET settlements'):
                resp = requests.get(url, headers=headers, params=params)

            resp.raise_for_status()

            batch_token = get_batch_token_from_headers(resp.headers)

            yield (resp.json(), batch_token)
