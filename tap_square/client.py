from square.client import Client
from singer import bookmarks

class SquareClient():
    def __init__(self, config):
        self._refresh_token = config['refresh_token']
        self._client_id = config['client_id']
        self._client_secret = config['client_secret']

        self._access_token = self._get_access_token()
        self._client = Client(access_token=self._access_token, environment='sandbox')

    def _get_access_token(self):
        body = {
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': self._refresh_token
        }

        result = client.o_auth.obtain_token(body)

        if result.is_success():
            return result.body['objects']['access_token']
        elif result.is_error():
            raise Exception(result.errors)

    def get_catalog_items(self, start_time):
        body = {
            "object_types": ["ITEM"],
            "include_deleted_objects": True,
            "query":  {
                "sorted_attribute_query": {
                    "attribute_name": "updated_at",
                    "initial_attribute_value": start_time,
                    "sort_order": "ASC"
                }
            }
        }

        result = self.client.catalog.search_catalog_objects(body=body).body

        if result.is_error():
            raise Exception(result.errors)

        yield result

        while result.get('cursor'):
            result = self.client.catalog.search_catalog_objects(body=body, cursor = result['cursor']).body

            if result.is_error():
                raise Exception(result.errors)

            yield result
