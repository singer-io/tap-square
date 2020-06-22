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


    def get_catalog_items(self, start_time, bookmarked_cursor):
        # Move the max_updated_at back the smallest unit possible
        # because the begin_time query param is exclusive
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
            "object_types": ["ITEM"],
            "include_deleted_objects": True,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET items'):
            result = self._client.catalog.search_catalog_objects(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('objects', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET items'):
                result = self._client.catalog.search_catalog_objects(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('objects'), result.body.get('cursor'))

    def get_catalog_categories(self, start_time, bookmarked_cursor):
        # Move the max_updated_at back the smallest unit possible
        # because the begin_time query param is exclusive
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
            "object_types": ["CATEGORY"],
            "include_deleted_objects": True,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET categories'):
            result = self._client.catalog.search_catalog_objects(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('objects', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET categories'):
                result = self._client.catalog.search_catalog_objects(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('objects'), result.body.get('cursor'))

    def get_catalog_discounts(self, start_time, bookmarked_cursor):
        # Move the max_updated_at back the smallest unit possible
        # because the begin_time query param is exclusive
        start_time = utils.strptime_to_utc(start_time)
        start_time = start_time - timedelta(milliseconds=1)
        start_time = utils.strftime(start_time)

        body = {
            "object_types": ["DISCOUNT"],
            "include_deleted_objects": True,
        }

        if bookmarked_cursor:
            body['cursor'] = bookmarked_cursor
        else:
            body['begin_time'] = start_time

        with singer.http_request_timer('GET discounts'):
            result = self._client.catalog.search_catalog_objects(body=body)

        if result.is_error():
            raise Exception(result.errors)

        yield (result.body.get('objects', []), result.body.get('cursor'))

        while result.body.get('cursor'):
            body['cursor'] = result.body['cursor']
            with singer.http_request_timer('GET discounts'):
                result = self._client.catalog.search_catalog_objects(body=body)

            if result.is_error():
                raise Exception(result.errors)

            yield (result.body.get('objects'), result.body.get('cursor'))
