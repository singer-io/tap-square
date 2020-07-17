import uuid
import random
import json
import os
import random
import requests
from datetime import datetime

import singer

from tap_square.client import SquareClient

LOGGER = singer.get_logger()


typesToKeyMap = {
    'ITEM': 'item_data',
    'CATEGORY': 'category_data',
    'DISCOUNT': 'discount_data',
    'TAX': 'tax_data'
}


class TestClient(SquareClient):
    """
    Client used to perfrom GET, CREATE and UPDATE on streams.
        NOTE: employees stream uses deprecated endpoints for CREATE and UPDATE
    """
    PAYMENTS = [] # We need to track state of records due to a dependency in the `refunds` stream

    stream_to_data_schema = {
        'items': {'type': 'ITEM',
                  'item_data': {'name': 'tap_tester_item_data'}},
        'categories': {'type': 'CATEGORY',
                       'category_data': {'name': 'tap_tester_category_data'}},
        'discounts': {'type': 'DISCOUNT',
                      'discount_data': {'name': 'tap_tester_discount_data',
                                        'discount_type': 'FIXED_AMOUNT',
                                        'amount_money': {'amount': 34500,
                                                         'currency': 'USD'}}},
        'taxes': {'type': 'TAX',
                  'tax_data': {'name': 'tap_tester_tax_data'}},
    }

    def __init__(self):
        config = {
            'refresh_token': os.getenv('TAP_SQUARE_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_SQUARE_APPLICATION_ID'),
            'client_secret': os.getenv('TAP_SQUARE_APPLICATION_SECRET'),
            'sandbox': 'true'
        }

        super().__init__(config)

    def get_all(self, stream, start_date=None, end_date=None):
        if stream == 'items':
            return [obj for page, _ in self.get_catalog('ITEM', start_date, None) for obj in page]
        elif stream == 'categories':
            return [obj for page, _ in self.get_catalog('CATEGORY', start_date, None) for obj in page]
        elif stream == 'discounts':
            return [obj for page, _ in self.get_catalog('DISCOUNT', start_date, None) for obj in page]
        elif stream == 'taxes':
            return [obj for page, _ in self.get_catalog('TAX', start_date, None) for obj in page]
        elif stream == 'employees':
            return [obj for page, _ in self.get_employees(None) for obj in page]
        elif stream == 'locations':
            return [obj for page, _ in self.get_locations() for obj in page]
        elif stream == 'refunds':
            return [obj for page, _ in self.get_refunds('REFUND', start_date, None) for obj in page]
        elif stream == 'payments':
            if not self.PAYMENTS:
                self.PAYMENTS += [obj for page, _ in self.get_payments('PAYMENT', start_date, None) for obj in page]
            return self.PAYMENTS
        elif stream == 'modifier_lists':
            return [obj for page, _ in self.get_catalog('MODIFIER_LIST', start_date, None) for obj in page]
        else:
            raise NotImplementedError

    def post_location(self, body):
        """
        body looks like

        {'location': {'address': {'address_line_1': '1234 Peachtree St. NE',
                                  'administrative_district_level_1': 'GA',
                                  'locality': 'Atlanta',
                                  'postal_code': '30309'},
                      'description': 'My new location.',
                      'name': 'New location name'}}

        where `address` and `description` are optional, but `name` is required
        """
        resp = self._client.locations.create_location(body)
        if resp.is_error():
            raise RuntimeError(resp.errors)
        location_id = resp.body.get('location', {}).get('id')
        location_name = resp.body.get('location', {}).get('name')
        LOGGER.info('Created location with id %s and name %s', location_id, location_name)
        return resp


    def post_category(self, body):
        """
        body:
        {
        'idempotency_key': <uuid>,
        'batches': [
        {'objects': [
        {'id': <required>,
        'type': <required>,
        'item_data': <CatalogItem, required for CatalogItem object>,
        'category_data': <CatalogCatalog, required for CatalogCategory object>,
        'discount_data': <CatalogDiscount, required for CatalogDiscount object>,
        'tax_data': <CatalogTax, required for CatalogTax object>,
        }
        ]
        }
        ]
        }
        """
        resp = self._client.catalog.batch_upsert_catalog_objects(body)
        if resp.is_error():
            stream_name = body['batches'][0]['objects'][0]['type']
            raise RuntimeError('Stream {}: {}'.format(stream_name, resp.errors))
        for obj in resp.body['objects']:
            category_id = obj.get('id')
            category_type = obj.get('type')
            category_name = obj.get(typesToKeyMap.get(category_type), {}).get('name', 'NONE')
            LOGGER.info('Created %s with id %s and name %s', category_type, category_id, category_name)
        return resp

    def create(self, stream, ext_obj=None, start_date=None):
        if stream == 'items':
            return self.create_item().body.get('objects')
        elif stream == 'categories':
            return self.create_categories().body.get('objects')
        elif stream == 'discounts':
            return self.create_discounts().body.get('objects')
        elif stream == 'taxes':
            return self.create_taxes().body.get('objects')
        elif stream == 'employees':
            return self.create_employees().body.get('objects')
        elif stream == 'locations':
            return [self.create_locations().body.get('location')]
        elif stream == 'refunds':
            return [self.create_refunds(ext_obj, start_date).body.get('refund')]
        elif stream == 'payments':
            return [self.create_payments().body.get('payment')]
        else:
            raise NotImplementedError

    def make_id(self, stream):
        return '#{}_{}'.format(stream, datetime.now().strftime('%Y%m%d%H%M%S%fZ'))

    def create_refunds(self, payment_obj=None, start_date=None): # IN_PROGRESS
        """This depends on an exisitng payment_obj, and will act as an UPDATE for a payment record."""
        if payment_obj is None:
            if not self.PAYMENTS: # if we have not called get_all on payments prior to this method call
                self.get_all('payments', start_date=start_date)#, start_date=start_date)

            for payment in self.PAYMENTS: # Find a payment with status: COMPLETED
               if payment.get('status') != "COMPLETED" or payment.get('refund_ids'):
                   continue
               payment_obj = payment
               break
        if payment_obj is None:
            raise Exception(
                "The are currently no payments in the test data set with status " + \
                "COMPLETED and cannot make a refund.")

        payment_id = payment_obj.get('id')
        payment_amount = payment_obj.get('amount_money').get('amount')
        upper_limit = 200 if 200 < payment_amount else payment_amount
        amount = random.randint(100, upper_limit)
        status = payment_obj.get('status')
        if status != "COMPLETED": # Just a sanity check that the for loop above is working
            raise Exception("You cannot refund a payment with status: {}".format(status))
        body = {'id': self.make_id('refund'),
                'payment_id': payment_id,
                'amount_money': {'amount': amount, # in cents
                                 'currency': 'USD'},
                'idempotency_key': str(uuid.uuid4()),
                'reason': 'Becuase you are worth it'}
        refund = self._client.refunds.refund_payment(body)
        if refund.is_error(): # Debugging
            print("body: {}".format(body))
            print("response: {}".format(refund))
        return refund

    def create_payments(self):
        body ={'id': self.make_id('payment'),
               'idempotency_key': str(uuid.uuid4()),
               'amount_money': {'amount': random.randint(100,10000), # in cents
                               'currency': 'USD'},
               'source_id': random.choice(['cnon:card-nonce-ok',  # card
                                           'cnon:card-nonce-ok',  # card on file
                                           'cnon:gift-card-nonce-ok'  # Square gift card
               ]),
               # 'app_fee_money': {'amount': 10,'currency': 'USD'}, # Insufficient permissions to set app_fee_money?
               'autocomplete': False,
               # 'customer_id': '',
               # 'location_id': '',
               # 'reference_id': '123456',
               'note': self.make_id('payment'),
        }
        return self._client.payments.create_payment(body)

    def create_item(self):
        body = {'batches': [{'objects': [{'id': self.make_id('item'),
                                          'type': 'ITEM',
                                          'item_data': {'name': self.make_id('item')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def create_categories(self):
        body = {'batches': [{'objects': [{'id': self.make_id('category'),
                                          'type': 'CATEGORY',
                                          'category_data': {'name': self.make_id('category')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def create_discounts(self):
        body = {'batches': [{'objects': [{'id': self.make_id('discount'),
                                          'type': 'DISCOUNT',
                                          'discount_data': {'name': self.make_id('discount'),
                                                            'discount_type': 'FIXED_AMOUNT',
                                                            'amount_money': {'amount': 34500,
                                                                             'currency': 'USD'}}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def create_taxes(self):
        body = {'batches': [{'objects': [{'id': self.make_id('tax'),
                                          'type': 'TAX',
                                          'tax_data': {'name': self.make_id('tax')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def create_locations(self):
        body = {'location': {'name': self.make_id('location')}}
        return self.post_location(body)

    def create_employees(self):
        # # TODO Either remove this or make every other stream refernce production
        # HEADERS = {
        #     "Authorization":"Bearer {}".format("{" + self._access_token + "}"),
        #     "Content-Type": "application/json"}
        # base_v1 = "https://connect.squareup.com/v1/me/"
        # # base_v1 = "https://connect.squareupsandbox.com/v1" # THIS DOES NOT EXIST
        # endpoint = "employees"
        # full_url = base_v1 + endpoint
        # body = {'id': self.make_id('employee'),
        #         'first_name': 'singer',
        #         'last_name': 'songerwriter'}
        # #'email': '{}@stitchdata.com'.format(self.make_id('employee')[1:].replace('_', '')),
        # response = requests.post(full_url, headers=HEADERS, data=body)
        # if response.status_code >= 400:
        #     print(response.text)
        #     import pdb; pdb.set_trace()
        # return response.json()
        return None

    def update(self, stream, obj_id, version):
        """For `stream` update `obj_id` with a new name

        We found that you have to send the same `obj_id` and `version` for the update to work
        """
        if stream == 'items':
            return self.update_item(obj_id, version).body.get('objects')
        elif stream == 'categories':
            return self.update_categories(obj_id, version).body.get('objects')
        elif stream == 'discounts':
            return self.update_discounts(obj_id, version).body.get('objects')
        elif stream == 'taxes':
            return self.update_taxes(obj_id, version).body.get('objects')
        elif stream == 'employees':
            return self.update_employees(obj_id, version).body.get('objects')
        elif stream == 'locations':
            return [self.update_locations(obj_id).body.get('location')]
        elif stream == 'payments':
            return [self.update_payment(obj_id).body.get('payment')]
        else:
            raise NotImplementedError

    def update_item(self, obj_id, version):
        body = {'batches': [{'objects': [{'id': obj_id,
                                          'type': 'ITEM',
                                          'item_data': {'name': self.make_id('item')},
                                          'version': version}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def update_payment(self, obj_id):
        """Cancel or a Complete an APPROVED payment"""
        body = {'payment_id': obj_id}
        action = random.choice([ 'complete', 'cancel' ])
        print("PAYMENT UPDATE: status for payment {} change to {} ".format(obj_id, action))
        if action == 'cancel':
            return self._client.payments.cancel_payment(obj_id)
        return self._client.payments.complete_payment(body=body, payment_id=obj_id)  # ew square

    def update_categories(self, obj_id, version):
        body = {'batches': [{'objects': [{'id': obj_id,
                                          'type': 'CATEGORY',
                                          'version': version,
                                          'category_data': {'name': self.make_id('category')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def update_discounts(self, obj_id, version):
        body = {'batches': [{'objects': [{'id': obj_id,
                                         'type': 'DISCOUNT',
                                          'version': version,
                                          'discount_data': {'name': self.make_id('discount'),
                                                            'discount_type': 'FIXED_AMOUNT',
                                                            'amount_money': {'amount': 34500,
                                                                             'currency': 'USD'}}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def update_taxes(self, obj_id, version):
        body = {'batches': [{'objects': [{'id': obj_id,
                                          'type': 'TAX',
                                          'version': version,
                                          'tax_data': {'name': self.make_id('tax')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def update_locations(self, obj_id):
        body = {'location': {'name': self.make_id('location')}}
        return self._client.locations.update_location(obj_id, body)

    def delete_catalog(self, ids_to_delete):
        body = {'object_ids': ids_to_delete}
        return self._client.catalog.batch_delete_catalog_objects(body)

    def create_batch_post(self, stream, num_records):
        recs_to_create = []
        for i in range(num_records):
            # Use dict() to make a copy so you don't get a list of the same object
            obj = dict(self.stream_to_data_schema[stream])
            obj['id'] = '#' + stream + str(i)
            recs_to_create.append(obj)
        body = {'idempotency_key': str(uuid.uuid4()),
                'batches': [{'objects': recs_to_create}]}
        return self.post_category(body)
