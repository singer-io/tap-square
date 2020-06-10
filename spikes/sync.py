from square.client import Client
from singer import utils
import time

def sync_via_list_catalog(config):
    client = Client(access_token=config['access_token'], environment = 'sandbox')

    catalog_api = client.catalog

    result = catalog_api.list_catalog(types='ITEM').body
    print('Length of first page: {}'.format(len(result['objects'])))
    print('Cursor for next page: {}'.format(result['cursor']))

    names = [obj['item_data']['name'] for obj in result['objects']]
    print('Order of objects returned: {}'.format(names))

    result2 = catalog_api.list_catalog(cursor=result['cursor'], types='ITEM').body
    print('Length of next page: {}'.format(len(result2.get('objects'))))
    print('Cursor for next page: {}'.format(result2.get('cursor', 'Not Found')))

    names2 = [obj['item_data']['name'] for obj in result2.get('objects')]
    print('Order of objects returned: {}'.format(names2))

    print('Number of unique objects: {}'.format(len(set(names + names2))))


def sync(config):
    client = Client(access_token=config['access_token'], environment = 'sandbox')

    catalog_api = client.catalog

    run3 = []

    run4 = []
    sync_with_query(catalog_api)
    sync_with_both(catalog_api)
    for i in range(10):

        start_time2 = time.time()
        sync_with_query(catalog_api)
        end_time2 = time.time()

        start_time1 = time.time()
        sync_with_both(catalog_api)
        end_time1 = time.time()

        run4.append(end_time1 - start_time1) # both
        run3.append(end_time2 - start_time2) # query


    run1 = []

    run2 = []

    for i in range(10):

        start_time1 = time.time()
        sync_with_both(catalog_api)
        end_time1 = time.time()

        start_time2 = time.time()
        sync_with_query(catalog_api)
        end_time2 = time.time()

        run1.append(end_time1 - start_time1) # both
        run2.append(end_time2 - start_time2) # query

    print('Avg time for sync_with_query: {} {}'.format((sum(run2)/10), (sum(run3)/10)))
    print('Avg time for sync_with_both: {} {}'.format((sum(run1)/10), (sum(run4)/10)))

def sync_with_both(catalog_api):
    result = catalog_api.search_catalog_objects(
      body = {
          "object_types": ["ITEM"],
          "include_deleted_objects": True,
          "limit": 100,
          "begin_time": "2020-06-09T7:00:00.000Z",
          "query": {
              "sorted_attribute_query": {
                  "attribute_name": "updated_at",
                  "initial_attribute_value": "2020-06-09T7:00:00.000Z",
                  "sort_order": "ASC"
              }
          }
      }
    ).body

    while result.get('cursor'):
        result = catalog_api.search_catalog_objects(
            body = {
                "object_types": ["ITEM"],
                "include_deleted_objects": True,
                "limit": 100,
                "begin_time": "2020-06-09T7:00:00.000Z",
                "query": {
                    "sorted_attribute_query": {
                        "attribute_name": "updated_at",
                        "initial_attribute_value": "2020-06-09T7:00:00.000Z",
                        "sort_order": "ASC"
                    }
                }
            },
            cursor = result['cursor']
        ).body

def sync_with_query(catalog_api):
    result = catalog_api.search_catalog_objects(
      body = {
          "object_types": ["ITEM"],
          "include_deleted_objects": True,
          "limit": 100,
          "query":  {
              "sorted_attribute_query": {
                  "attribute_name": "updated_at",
                  "initial_attribute_value": "2020-06-09T7:00:00.000Z",
                  "sort_order": "ASC"
              }
          }
      }
    ).body

    if result._error:
        raise Exception

    while result.get('cursor'):
        result = catalog_api.search_catalog_objects(
            body = {
                "object_types": ["ITEM"],
                "include_deleted_objects": True,
                "limit": 100,
                "query": {
                    "sorted_attribute_query": {
                        "attribute_name": "updated_at",
                        "initial_attribute_value": "2020-06-09T7:00:00.000Z",
                        "sort_order": "ASC"
                    }
                }
            },
            cursor = result['cursor']
        ).body


def main():
    args = utils.parse_args([])

    sync(args.config)


if __name__ == '__main__':
    main()
