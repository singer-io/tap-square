from datetime import timedelta
import urllib.parse

from square.client import Client
from singer import utils
import singer
import requests
import backoff


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

def should_not_retry(ex):
    """
    Marks certain exception types (e.g., 400) as non-retryable
    """
    if hasattr(ex, "response") and \
       hasattr(ex.response, "status_code") and \
       ex.response.status_code in {400, 401}:
        return True
    return False


def log_backoff(details):
    '''
    Logs a backoff retry message
    '''
    LOGGER.warning('Error receiving data from square. Sleeping %.1f seconds before trying again', details['wait'])


class RetryableError(Exception):
    pass


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
            raise RuntimeError(error_message)

        return result.body['access_token']

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        RetryableError,
        max_time=180, # seconds
        giveup=should_not_retry,
        on_backoff=log_backoff,
        jitter=backoff.full_jitter,
    )
    def _retryable_v2_method(request_method, body, **kwargs):
        result = request_method(body, **kwargs)

        if result.is_error():
            error_message = result.errors if result.errors else result.body
            if 'Service Unavailable' in error_message or 'upstream connect error or disconnect/reset before headers' in error_message or result.status_code == 429:
                raise RetryableError(error_message)
            else:
                raise RuntimeError(error_message)

        return result

    def _get_v2_objects(self, request_timer_suffix, request_method, body, body_key):
        cursor = body.get('cursor', '__initial__')
        while cursor:
            if cursor != '__initial__':
                body['cursor'] = cursor

            with singer.http_request_timer('GET ' + request_timer_suffix):
                result = self._retryable_v2_method(request_method, body)

            cursor = result.body.get('cursor')
            yield (result.body.get(body_key, []), cursor)


    def get_catalog(self, object_type, start_time):
        # Move the max_updated_at back the smallest unit possible
        # because the begin_time query param is exclusive
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
            "object_types": [object_type],
            "include_deleted_objects": True,
        }

        body['begin_time'] = start_time

        yield from self._get_v2_objects(
            object_type,
            lambda bdy: self._client.catalog.search_catalog_objects(body=bdy),
            body,
            'objects')

    def get_employees(self, bookmarked_cursor):
        body = {
            'limit': 50,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        yield from self._get_v2_objects(
            'employees',
            lambda bdy: self._client.employees.list_employees(**bdy),
            body,
            'employees')

    def get_locations(self):
        body = {}

        yield from self._get_v2_objects(
            'locations',
            lambda bdy: self._client.locations.list_locations(**bdy),
            body,
            'locations')

    def get_bank_accounts(self):
        body = {}

        yield from self._get_v2_objects(
            'bank_accounts',
            lambda bdy: self._client.bank_accounts.list_bank_accounts(**bdy),
            body,
            'bank_accounts')

    def get_customers(self, start_time, end_time):
        body = {
            "query": {
                "filter": {
                    "updated_at": {
                        "start_at": start_time, # Inclusive on start_at
                        'end_at': end_time      # Exclusive on end_at
                    }
                },
            }
        }

        yield from self._get_v2_objects(
            'customers',
            lambda bdy: self._client.customers.search_customers(body=bdy),
            body,
            'customers')

    def get_orders(self, location_ids, start_time):
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

        yield from self._get_v2_objects(
            'orders',
            lambda bdy: self._client.orders.search_orders(body=bdy),
            body,
            'orders')

    def get_inventories(self, start_time, bookmarked_cursor):
        body = {'updated_after': start_time}

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        yield from self._get_v2_objects(
            'inventories',
            lambda bdy: self._client.inventory.batch_retrieve_inventory_counts(body=bdy),
            body,
            'counts')

    def get_shifts(self, bookmarked_cursor):
        body = {
            "query": {
                "sort": {
                    "field": "UPDATED_AT",
                    "order": "ASC"
                }
            }
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        yield from self._get_v2_objects(
            'shifts',
            lambda bdy: self._client.labor.search_shifts(body=bdy),
            body,
            'shifts')

    def get_refunds(self, start_time, bookmarked_cursor):
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
        }
        body['begin_time'] = start_time

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        yield from self._get_v2_objects(
            'refunds',
            lambda bdy: self._client.refunds.list_payment_refunds(**bdy),
            body,
            'refunds')

    def get_payments(self, start_time, bookmarked_cursor):
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
        }
        body['begin_time'] = start_time

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor

        yield from self._get_v2_objects(
            'payments',
            lambda bdy: self._client.payments.list_payments(**bdy),
            body,
            'payments')

    def get_cash_drawer_shifts(self, location_id, start_time, bookmarked_cursor):
        if bookmarked_cursor:
            cursor = bookmarked_cursor
        else:
            cursor = '__initial__' # initial value so while loop is always entered one time

        end_time = utils.strftime(utils.now(), utils.DATETIME_PARSE)
        while cursor:
            if cursor == '__initial__':
                # initial text was needed to go into the while loop, but api needs
                # it to be a valid bookmarked cursor or None
                cursor = bookmarked_cursor

            with singer.http_request_timer('GET cash drawer shifts'):
                result = self._retryable_v2_method(
                    lambda bdy: self._client.cash_drawers.list_cash_drawer_shifts(
                        location_id=location_id,
                        begin_time=start_time,
                        end_time=end_time,
                        cursor=cursor,
                        limit=1000,
                    ),
                    None,
                )

            yield (result.body.get('items', []), result.body.get('cursor'))

            cursor = result.body.get('cursor')

    def _get_v1_objects(self, url, params, request_timer_suffix, bookmarked_cursor):
        headers = {
            'content-type': 'application/json',
            'authorization': 'Bearer {}'.format(self._access_token)
        }

        if bookmarked_cursor:
            batch_token = bookmarked_cursor
        else:
            batch_token = '__initial__'

        session = requests.Session()
        session.headers.update(headers)

        while batch_token:
            if batch_token != '__initial__':
                params['batch_token'] = batch_token

            with singer.http_request_timer('GET ' + request_timer_suffix):
                result = self._retryable_v1_method(session, url, params)

            batch_token = get_batch_token_from_headers(result.headers)

            yield (result.json(), batch_token)

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=180, # seconds
        giveup=should_not_retry,
        on_backoff=log_backoff,
        jitter=backoff.full_jitter,
    )
    def _retryable_v1_method(session, url, params):
        result = session.get(url, params=params)
        result.raise_for_status()

        return result

    def get_roles(self, bookmarked_cursor):
        yield from self._get_v1_objects(
            'https://connect.squareup.com/v1/me/roles',
            dict(),
            'roles',
            bookmarked_cursor,
        )

    def get_settlements(self, location_id, start_time, bookmarked_cursor):
        url = 'https://connect.squareup.com/v1/{}/settlements'.format(location_id)

        now = utils.now()
        start_time_dt = utils.strptime_to_utc(start_time)
        end_time_dt = now

        # Parameter `begin_time` cannot be before 1 Jan 2013 00:00:00Z
        # Doc: https://developer.squareup.com/reference/square/settlements-api/v1-list-settlements
        if start_time_dt < utils.strptime_to_utc("2013-01-01T00:00:00Z"):
            raise Exception("Start Date for Settlements stream cannot come before `2013-01-01T00:00:00Z`, current start_date: {}".format(start_time))

        while start_time_dt < now:
            params = {
                'limit': 200,
                'begin_time': utils.strftime(start_time_dt),
            }
            # If query range is over a year, shorten to a year
            if now - start_time_dt > timedelta(weeks=52):
                end_time_dt = start_time_dt + timedelta(weeks=52)
                params['end_time'] = utils.strftime(end_time_dt)
            yield from self._get_v1_objects(
                url,
                params,
                'settlements',
                bookmarked_cursor,
            )
            # Attempt again to sync til "now"
            start_time_dt = end_time_dt
            end_time_dt = now
