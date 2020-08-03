import uuid
import random
import json
import os
import random
import requests
from datetime import datetime, timedelta

import singer

from tap_square.client import SquareClient
from tap_square.streams import Inventories, Orders, chunks, Settlements, CashDrawerShifts

LOGGER = singer.get_logger()


typesToKeyMap = {
    'ITEM': 'item_data',
    'CATEGORY': 'category_data',
    'DISCOUNT': 'discount_data',
    'TAX': 'tax_data',
    'MODIFIER_LIST': 'modifier_list_data'
}


class TestClient(SquareClient):
    # This is the duration of a shift, we make this constant so we can
    # ensure the shifts don't overlap
    SHIFT_MINUTES = 2

    """
    Client used to perfrom GET, CREATE and UPDATE on streams.
        NOTE: employees stream uses deprecated endpoints for CREATE and UPDATE
    """
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
        'modifier_lists': {'type': 'MODIFIER_LIST',
                           'modifier_list_data': {'name': 'tap_tester_modifier_list_data',
                                                  'ordinal': 1,
                                                  'selection_type': random.choice(['SINGLE', 'MULTIPLE']),
                                                  "modifiers": [
                                                      {'type': 'MODIFIER',
                                                       'modifier_data': {
                                                           'name': 'tap_tester_modifier_data',
                                                           'price_money': {
                                                               'amount': 300,
                                                               'currency': 'USD'},
                                                       },
                                                       'ordinal': 1}]}},
    }

    def __init__(self, env):
        config = {
            'refresh_token': os.getenv('TAP_SQUARE_REFRESH_TOKEN') if env == 'sandbox' else os.getenv('TAP_SQUARE_PROD_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_SQUARE_APPLICATION_ID') if env == 'sandbox' else os.getenv('TAP_SQUARE_PROD_APPLICATION_ID'),
            'client_secret': os.getenv('TAP_SQUARE_APPLICATION_SECRET') if env == 'sandbox' else os.getenv('TAP_SQUARE_PROD_APPLICATION_SECRET'),
            'sandbox' : 'true' if env  == 'sandbox' else 'false',
        }

        super().__init__(config)

    ##########################################################################
    ### GETs
    ##########################################################################

    def get_all(self, stream, start_date=None):
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
        elif stream == 'bank_accounts':
            return [obj for page, _ in self.get_bank_accounts() for obj in page]
        elif stream == 'refunds':
            return [obj for page, _ in self.get_refunds(start_date, None) for obj in page]
        elif stream == 'payments':
            return [obj for page, _ in self.get_payments(start_date, None) for obj in page]
        elif stream == 'modifier_lists':
            return [obj for page, _ in self.get_catalog('MODIFIER_LIST', start_date, None) for obj in page]
        elif stream == 'inventories':
            return [obj for page, _ in self.get_inventories(start_date, None) for obj in page]
        elif stream == 'orders':
            orders = Orders()
            return [obj for page, _ in orders.sync(self, start_date, None) for obj in page]
        elif stream == 'roles':
            return [obj for page, _ in self.get_roles(None) for obj in page]
        elif stream == 'shifts':
            return [obj for page, _ in self.get_shifts(start_date) for obj in page]
        elif stream == 'settlements':
            settlements = Settlements()
            return [obj for page, _ in settlements.sync(self, start_date) for obj in page]
        elif stream == 'cash_drawer_shifts':
            cash_drawer_shifts = CashDrawerShifts()
            return [obj for page, _ in cash_drawer_shifts.sync(self, start_date, None) for obj in page]
        else:
            raise NotImplementedError("Not implemented for stream {}".format(stream))

    def get_a_payment(self, payment_id, start_date):
        return_value = []
        while not return_value:
            LOGGER.info('get_a_payment: Calling API')
            all_payments = self.get_all('payments', start_date)
            return_value = [payment for payment in all_payments if payment['id'] == payment_id]

            return_value = None if len(return_value) > 0 and return_value[0].get('status') == 'APPROVED' else return_value

        LOGGER.info('get_a_payment: %s', str(return_value))
        return return_value

    ##########################################################################
    ### CREATEs
    ##########################################################################

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
            LOGGER.debug('Created %s with id %s and name %s', category_type, category_id, category_name)
        return resp

    def post_order(self, body, location_id):
        """
        body: {
          "order": {
            "location_id": None,
          },
          "idempotency_key": str(uuid.uuid4())
        }
        """
        resp = self._client.orders.create_order(location_id=location_id, body=body)

        if resp.is_error():
            stream_name = body['batches'][0]['objects'][0]['type']
            raise RuntimeError('Stream {}: {}'.format(stream_name, resp.errors))
        LOGGER.info('Created Order with id %s', resp.body['order'].get('id'))
        return resp

    def create(self, stream, start_date=None, end_date=None, num_records=1):
        if not start_date:
            raise ValueError("Expected start_date but None was provided")

        if stream == 'items':
            return self._create_item(start_date=start_date, num_records=num_records).body.get('objects')
        elif stream == 'categories':
            return self.create_categories().body.get('objects')
        elif stream == 'discounts':
            return self.create_discounts().body.get('objects')
        elif stream == 'taxes':
            return self.create_taxes().body.get('objects')
        elif stream == 'modifier_lists':
            return self.create_modifier_list().body.get('objects')
        elif stream == 'employees':
            return self.create_employees().body.get('objects')
        elif stream == 'inventories':
            return self.create_batch_inventory_adjustment(start_date=start_date)
        elif stream == 'locations':
            return [self.create_locations().body.get('location')]
        elif stream == 'orders':
            location_id = [location['id'] for location in self.get_all('locations')][0]
            return [self.create_order(location_id).body.get('order')]
        elif stream == 'refunds':
            (created_refund, _) = self.create_refund(start_date)
            return [created_refund.body.get('refund')]
        elif stream == 'payments':
            return [self.create_payments()]
        elif stream == 'shifts':
            employee_id = [employee['id'] for employee in self.get_all('employees')][0]
            location_id = [location['id'] for location in self.get_all('locations')][0]
            max_end_at = max([obj['end_at'] for obj in self.get_all('shifts')])
            if start_date < max_end_at:
                LOGGER.warning('Tried to create a Shift that overlapped another shift')
                # Readjust start date and end date
                start_date = max_end_at

            if not end_date:
                start_date_parsed = singer.utils.strptime_with_tz(start_date)
                end_date_datetime = start_date_parsed + timedelta(minutes = self.SHIFT_MINUTES)
                end_date = singer.utils.strftime(end_date_datetime)

            return [self.create_shift(employee_id, location_id, start_date, end_date).body.get('shift')]
        else:
            raise NotImplementedError("create not implemented for stream {}".format(stream))

    def make_id(self, stream):
        return '#{}_{}'.format(stream, datetime.now().strftime('%Y%m%d%H%M%S%fZ'))

    # TODO Go through each stream and ensure we are creating as many fiedls within records as possible
    def get_catalog_object(self, obj_id):
        response = self._client.catalog.retrieve_catalog_object(object_id=obj_id)
        if response.is_error():
            raise RuntimeError(response.errors)

        return response.body.get('object')

    def get_an_inventory_adjustment(self, obj_id):
        response = self._client.inventory.retrieve_inventory_changes(catalog_object_id=obj_id)

        if response.is_error():
            raise RuntimeError('GET INVENTORY_ADJUSTMENT: {}'.format(response.errors))
        return response

    def create_batch_inventory_adjustment(self, start_date, num_records=1):
        # Create an item
        items = self._create_item(start_date, num_records).body.get('objects', [])
        assert(items)

        # Crate an item_variation and get it's ID
        item_variations = self.create_item_variation([item.get('id') for item in items]).body.get('objects', [])
        assert(item_variations)

        all_locations = self.get_all('locations')
        changes = []
        for item_variation in item_variations:
            catalog_obj_id = item_variation.get('id')

            # Get a random location
            loc_id =  all_locations[random.randint(0, len(all_locations) - 1)].get('id')
            from_state = 'IN_STOCK'  # inventory_obj.get('state')

            # Adjustment logic
            if from_state == 'IN_STOCK':
                states = ['SOLD', 'WASTE'] # SOLD_ONLINE
            else:
                states = ['CUSTOM', 'IN_STOCK', 'RETURNED_BY_CUSTOMER', 'RESERVED_FROM_SALE',
                        'ORDERED_FROM_VENDOR', 'RECEIVED_FROM_VENDOR',
                        'IN_TRANSIT_TO','UNLINKED_RETURN', 'NONE']
            to_state = random.choice(states)
            occurred_at = datetime.strftime(
                datetime.utcnow()-timedelta(hours=random.randint(1,23)), '%Y-%m-%dT%H:00:00Z')
            change = {
                'type': 'ADJUSTMENT',
                'adjustment': {
                    'from_state': from_state,
                    'to_state': to_state,
                    'location_id': loc_id,
                    'occurred_at': occurred_at,
                    'catalog_object_id': catalog_obj_id,
                    'quantity': '1.0',
                    'source': {
                        'product': random.choice([
                            'SQUARE_POS', 'EXTERNAL_API', 'BILLING', 'APPOINTMENTS',
                            'INVOICES', 'ONLINE_STORE', 'PAYROLL', 'DASHBOARD',
                            'ITEM_LIBRARY_IMPORT', 'OTHER'])}},
            }
            changes.append(change)

        all_counts = []
        for change_chunk in chunks(changes, 100):
            body = {
                'changes': change_chunk,
                'ignore_unchanged_counts': random.choice([True, False]),
                'idempotency_key': str(uuid.uuid4())
            }
            LOGGER.info("About to create %s inventory adjustments", len(change_chunk))
            response = self._client.inventory.batch_change_inventory(body)
            if response.is_error():
                LOGGER.error("response had error, body was %s", body)
                raise RuntimeError(response.errors)

            all_counts += response.body.get('counts')

        return all_counts

    def create_refund(self, start_date):
        """
        Create a refund object. This depends on an exisitng payment record, and will
        act as an UPDATE for a payment record. We can only refund payments whose status is
        COMPLETED and who have not been refunded previously. In some cases we will need to
        CREATE a new payment to achieve a refund.

        : param payment_obj: payment record is needed to reference 'id', 'status', and 'amount'
        : param start_date: this is requrired if we have not set state for PAYMENTS prior to the execution of this method
        """
        # SETUP
        payment_response = self.create_payments(autocomplete=True, source_key="card")
        payment_obj = self.get_a_payment(payment_id=payment_response.get('id'), start_date=start_date)[0]

        payment_id = payment_obj.get('id')
        payment_amount = payment_obj.get('amount_money').get('amount')
        upper_limit = 10 if 10 < payment_amount else payment_amount
        amount = random.randint(1, upper_limit)  # we must be careful not to refund more than the charge
        status = payment_obj.get('status')

        if status != "COMPLETED": # Just a sanity check that logic above is working
            raise Exception("You cannot refund a payment with status: {}".format(status))
        # REQUEST
        body = {'id': self.make_id('refund'),
                'payment_id': payment_id,
                'amount_money': {'amount': amount, # in cents
                                 'currency': 'USD'},
                'idempotency_key': str(uuid.uuid4()),
                'reason': 'Becuase you are worth it'}

        refund = self._client.refunds.refund_payment(body)
        if refund.is_error():
            print("Refund error, Updating payment status and retrying refund process.")

            if "PENDING_CAPTURE" in refund.body.get('errors')[0].get('detail'):
                payment = self.update_payment(obj_id=payment_id, action='complete').body.get('payment') # update (complete) a payment if it is pending
                body['idempotency_key'] = str(uuid.uuid4())
                body['id'] = self.make_id('refund')

                refund = self._client.refunds.refund_payment(body)
                if refund.is_error(): # Debugging
                    print("body: {}".format(body))
                    print("response: {}".format(refund))
                    print("payment attempted to be refunded: {}".format(payment))
                    raise RuntimeError(refund.errors)
            else:
                raise RuntimeError(refund.errors)

        return (refund, payment_obj)

    def create_payments(self, autocomplete=False, source_key=None):
        """
        Generate a pyament object
        : param autocomplete: boolean
        : param source_key: must be a key to the source dict below
        """
        source = {'card': 'cnon:card-nonce-ok',
                  'card_on_file': 'cnon:card-nonce-ok',
                  'gift_card': 'cnon:gift-card-nonce-ok'}

        if source_key:
            source_id = source.get(source_key)
        else:
            source_id = random.choice(list(source.values()))
        body ={'id': self.make_id('payment'),
               'idempotency_key': str(uuid.uuid4()),
               'amount_money': {'amount': random.randint(100,10000), # in cents
                               'currency': 'USD'},
               'source_id': source_id,
               # 'app_fee_money': {'amount': 10,'currency': 'USD'}, # Insufficient permissions to set app_fee_money?
               'autocomplete': autocomplete,
               # 'customer_id': '',
               # 'location_id': '',
               # 'reference_id': '123456',
               'note': self.make_id('payment'),}
        new_payment = self._client.payments.create_payment(body)
        if new_payment.is_error():
            print("body: {}".format(body))
            print("response: {}".format(new_payment))
            raise RuntimeError(new_payment.errors)

        response = new_payment.body.get('payment')
        return response

    def create_modifier_list(self):
        mod_id = self.make_id('modifier')
        list_id = self.make_id('modifier_lists')
        body = {'batches': [{'objects': [{'id': list_id,
                                          'type': 'MODIFIER_LIST',
                                          'modifier_list_data': {'name': list_id,
                                                                 'ordinal': 1,
                                                                 'selection_type': random.choice(['SINGLE', 'MULTIPLE']),
                                                                 "modifiers": [
                                                                     {'id': mod_id,
                                                                      'type': 'MODIFIER',
                                                                      'modifier_data': {
                                                                          'name': mod_id[1:],
                                                                          'price_money': {
                                                                              'amount': 300,
                                                                              'currency': 'USD'},
                                                                      },
                                                                      'modifier_list_id': list_id,
                                                                      'ordinal': 1}
                                                                 ],}
                                          }]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def _create_item(self, start_date, num_records=1):
        mod_lists = self.get_all('modifier_lists', start_date)
        mod_list_id = random.choice(mod_lists).get('id')

        item_ids = [self.make_id('item') for n in range(num_records)]
        objects = [{'id': item_id,
                    'type': 'ITEM',
                    'item_data': {'name': item_id,
                                  'modifier_list_info':[{
                                      'modifier_list_id': mod_list_id,
                                      'enabled': True
                                  }]}} for item_id in item_ids]
        body = {'batches': [{'objects': objects}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def create_item_variation(self, item_ids):
        objects = [{
            'id': self.make_id('item_variation'),
            'type': 'ITEM_VARIATION',
            'item_variation_data': {
                'item_id': item_id,
                'name': 'item data',
                'sku': 'sku ',
                'pricing_type': 'VARIABLE_PRICING',
                'track_inventory': True,
                'inventory_alert_type': 'LOW_QUANTITY',
                'user_data': 'user data'
            },
            'present_at_all_locations': random.choice([True, False]),
        } for item_id in item_ids]
        body = {'batches': [{'objects': objects}],
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

    def create_locations(self):
        made_id = self.make_id('location')
        website =  'https://get.stitchdata.com/stitch?utm_source=google' + \
            '&utm_medium=cpc&utm_campaign=stitch_ga_nam_en_dg_search_brand&utm_content' + \
            '=&utm_device=c&campaignid=10338160950&adgroupid=102105232759&gclid=' + \
            'EAIaIQobChMIivPt7f3j6gIVuwiICR1O_g6VEAAYAyAAEgJ2F_D_BwE'
        body = {'location': {'name': made_id,
                             'timezone': 'UTC',
                             'status': 'ACTIVE', # 'INACTIVE'
                             'language_code': 'en-US',
                             'phone_number': '9999999999',
                             'business_name': made_id,
                             'type': random.choice(['PHYSICAL', 'MOBILE']),
                             'business_hours': {
                               'periods': [{'day_of_week': 'SUN',
                                            'start_local_time': '10:00:00',
                                            'end_local_time': '15:00:00',},
                                           {'day_of_week': 'MON',
                                            'start_local_time': '09:00:00',
                                            'end_local_time': '17:00:00',},
                                           {'day_of_week': 'TUE',
                                            'start_local_time': '09:00:00',
                                            'end_local_time': '17:00:00',},
                                           {'day_of_week': 'WED',
                                            'start_local_time': '09:00:00',
                                            'end_local_time': '17:00:00',},
                                           {'day_of_week': 'THU',
                                            'start_local_time': '09:00:00',
                                            'end_local_time': '17:00:00',},
                                           {'day_of_week': 'FRI',
                                            'start_local_time': '09:00:00',
                                            'end_local_time': '17:00:00',},
                                           {'day_of_week': 'SAT',
                                            'start_local_time': '10:00:00',
                                            'end_local_time': '15:00:00',},]
                             },
                             'business_email': 'fake_business@sttichdata.com',
                             'description': 'This is a descriptino',
                             'twitter_username': 'twitteruser',
                             'instagram_username': 'iguser',
                             'website_url': website,
                             'coordinates': {
                                 'latitude': 39.951130,
                                 'longitude': -75.163120}}
        }
        response =  self.post_location(body)
        if response.is_error():
            print("body: {}".format(body))
            print("response: {}".format(new_payment))
            raise RuntimeError(response.errors)
        return response

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

    def create_order(self, location_id):
        body = {'order': {'location_id': None},
                'idempotency_key': str(uuid.uuid4())}
        return self.post_order(body, location_id)

    def create_shift(self, employee_id, location_id, start_date, end_date):
        body = {
            'idempotency_key': str(uuid.uuid4()),
            'shift': {
                'employee_id': employee_id, # This is the only employee we have on the sandbox
                'location_id': location_id, # Should we vary this?
                'start_at': start_date, # This can probably be derived from the test's start date
                'end_at': end_date # This can be some short time after the start time
            }
        }

        resp = self._client.labor.create_shift(body=body)

        if resp.is_error():
            raise RuntimeError(resp.errors)
        LOGGER.info('Created a Shift with id %s', resp.body.get('shift',{}).get('id'))
        return resp

    def create_batch_post(self, stream, num_records):
        recs_to_create = []
        for i in range(num_records):
            # Use dict() to make a copy so you don't get a list of the same object
            obj = dict(self.stream_to_data_schema[stream])
            obj_id = self.make_id(stream)
            obj['id'] = obj_id
            if stream == 'modifier_lists':
                obj['ordinal'] = i
                obj['modifier_list_id'] = obj_id
                obj['modifier_list_data']['modifier_data']['id'] = self.make_id('modifier')
                obj['modifier_list_data']['modifier_data']['ordinal'] = i
            recs_to_create.append(obj)
        body = {'idempotency_key': str(uuid.uuid4()),
                'batches': [{'objects': recs_to_create}]}
        return self.post_category(body)

    ##########################################################################
    ### UPDATEs
    ##########################################################################

    def update(self, stream, obj_id, version, obj=None):
        """For `stream` update `obj_id` with a new name

        We found that you have to send the same `obj_id` and `version` for the update to work
        """
        if not obj_id and not obj:
            raise RuntimeError("Require non-blank obj_id or a non-blank obj, found {} and ".format(obj_id, obj))

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
        elif stream == 'modifier_lists':
            raise NotImplementedError("{} is not implmented".format(stream))
        elif stream == 'inventories':
            return self.update_inventory_adjustment(obj).body.get('counts')
        elif stream == 'locations':
            return [self.update_locations(obj_id).body.get('location')]
        elif stream == 'orders':
            location_id = [location['id'] for location in self.get_all('locations')][0]
            return [self.update_order(location_id, obj_id, version).body.get('order')]
        elif stream == 'payments':
            return [self.update_payment(obj_id).body.get('payment')]
        elif stream == 'shifts':
            return [self.update_shift(obj).body.get('shift')]
        else:
            raise NotImplementedError("{} is not implmented".format(stream))

    def update_item(self, obj_id, version):
        if not obj_id:
            raise RuntimeError("Require non-blank obj_id, found {}".format(obj_id))
        body = {'batches': [{'objects': [{'id': obj_id,
                                          'type': 'ITEM',
                                          'item_data': {'name': self.make_id('item')},
                                          'version': version}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def update_payment(self, obj_id: str, action=None):
        """Cancel or a Complete an APPROVED payment"""
        if not obj_id:
            raise RuntimeError("Require non-blank obj_id, found {}".format(obj_id))

        if not action:
            action = random.choice([ 'complete', 'cancel' ])
        print("PAYMENT UPDATE: status for payment {} change to {} ".format(obj_id, action))
        if action == 'cancel':
            resp = self._client.payments.cancel_payment(obj_id)
            if resp.is_error():
                raise RuntimeError(resp.errors)
            return resp
        elif action == 'complete':
            body = {'payment_id': obj_id}
            resp = self._client.payments.complete_payment(body=body, payment_id=obj_id)  # ew square
            if resp.is_error():
                raise RuntimeError(resp.errors)

            return resp
        else:
            raise NotImplementedError('action {} not supported'.format(action))

    def update_categories(self, obj_id, version):
        body = {'batches': [{'objects': [{'id': obj_id,
                                          'type': 'CATEGORY',
                                          'version': version,
                                          'category_data': {'name': self.make_id('category')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)


    def update_modifier_list(self, obj): # TODO try v1 endpoint in produciton env
        body = {'batches': [{'objects': [{'id': obj_id,
                                         'type': 'MODIFIER_LIST',
                                          'version': version,
                                          'modifier_list_data': {'name': self.make_id('modifier_list')}}]}],
                'idempotency_key': str(uuid.uuid4())}
        return self.post_category(body)

    def _update_items_by_modifier_list(self, obj_id):  # REFACTOR
        # TODO use this as an update for the 'items' stream, not 'modifier_lists'

        # Get all items
        start_date = '2020-07-29T00:00:00Z'
        items = self.get_all('items', start_date)

        # Randomly select an item with an assigned modifier_list
        items_with_mods = [i for i in items if i.get('item_data', {'modifier_list_info': False}).get('modifier_list_info')]
        item = random.choice(items_with_mods)

        # Set update base on existing data
        item_data = item.get('item_data')
        enabled = item_data.get('modifier_list_info')[0].get('enabled')
        modifier_list_id = item_data.get('modifier_list_info')[0].get('modifier_list_id')
        modifier = 'modifier_lists_to_disable' if enabled else 'modifier_lists_to_enable'

        body = {'item_ids': [item.get('id')],
                modifier: [modifier_list_id],
        }

        resp = self._client.catalog.update_item_modifier_lists(body)
        if resp.is_error():
            raise RuntimeError("Unable to UPDATE modifier_lists: {}".format(resp.errors))
        # Updated modifier_lists return only the 'updated_at' in the response
        # so we are grabbing the response body from a get to copare
        mod_lists = self.get_all('modifier_lists', start_date)
        updated_mod_lists = [ml for ml in mod_lists if ml.get('id') == modifier_list_id]
        assert len(updated_mod_lists) == 1
        return updated_mod_lists

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
        resp = self._client.locations.update_location(obj_id, body)
        if resp.is_error():
            raise RuntimeError(resp.errors)
        return resp

    def update_inventory_adjustment(self, catalog_obj):
        catalog_obj_id = catalog_obj.get('catalog_object_id')
        loc_id = catalog_obj.get('location_id')

        from_state = 'IN_STOCK'  # inventory_obj.get('state')

        # Adjustment logic
        made_id = self.make_id('inventory')
        if from_state == 'IN_STOCK':
            states = ['SOLD', 'WASTE'] # SOLD_ONLINE
        else:
            states = ['CUSTOM', 'IN_STOCK', 'RETURNED_BY_CUSTOMER', 'RESERVED_FROM_SALE',
                      'ORDERED_FROM_VENDOR', 'RECEIVED_FROM_VENDOR',
                      'IN_TRANSIT_TO','UNLINKED_RETURN', 'NONE']
        to_state = random.choice(states)
        occurred_at = datetime.strftime(
            datetime.utcnow()-timedelta(hours=random.randint(1,23)), '%Y-%m-%dT%H:00:00Z')
        changes = {
            'ADJUSTMENT': {
                'adjustment': {
                    # 'id': made_id,
                    # 'from_state': random.choice(states),
                    'from_state': from_state,
                    'to_state': to_state,
                    'location_id': loc_id,
                    'occurred_at': occurred_at,
                    # 'employee_id': 'asdasd',
                    'catalog_object_id': catalog_obj_id,
                    # 'catalog_object_type': 'ITEM_VARIATION',
                    'quantity': '1.0',
                    'source': {
                        'product': random.choice([
                            'SQUARE_POS', 'EXTERNAL_API', 'BILLING', 'APPOINTMENTS',
                            'INVOICES', 'ONLINE_STORE', 'PAYROLL', 'DASHBOARD',
                            'ITEM_LIBRARY_IMPORT', 'OTHER'])}}},
            'PHYSICAL_COUNT': {
                'physical_count': {},
            },
        }
        change_type = 'ADJUSTMENT' # random.choice(list(changes.items()))
        key = [k for k in changes.get(change_type).keys()][0]
        value = [v for v in changes.get(change_type).values()][0]

        body = {
            'changes': [{'type': change_type,
                         key: value}],
            'ignore_unchanged_counts': random.choice([True, False]),
            'idempotency_key': str(uuid.uuid4())
        }
        response = self._client.inventory.batch_change_inventory(body)
        if response.is_error():
            print(response.body.get('errors'))
            raise RuntimeError(response.errors)

        return response

    def update_shift(self, obj):
        body = {
            "shift": { # TODO: Can this be obj with the title updated?
                "employee_id": obj['employee_id'],
                "location_id": obj['location_id'],
                "start_at": obj['start_at'],
                "end_at": obj['end_at'],
                "wage": {
                    "title": self.make_id('shift'),
                    "hourly_rate" : obj['wage']['hourly_rate']
                }
            }
        }
        resp = self._client.labor.update_shift(id=obj['id'], body=body)

        if resp.is_error():
            raise RuntimeError(resp.errors)
        LOGGER.info('Updated a Shift with id %s', resp.body.get('shift',{}).get('id'))
        return resp

    def update_order(self, location_id, obj_id, version):
        body = {'order': {'note': self.make_id('order'),
                          'version': version},
                'idempotency_key': str(uuid.uuid4())}
        resp = self._client.orders.update_order(location_id=location_id, order_id=obj_id, body=body)
        if resp.is_error():
            raise RuntimeError(resp.errors)
        return resp

    ##########################################################################
    ### DELETEs
    ##########################################################################

    def delete_catalog(self, ids_to_delete):
        body = {'object_ids': ids_to_delete}
        resp = self._client.catalog.batch_delete_catalog_objects(body)
        if resp.is_error():
            raise RuntimeError(resp.errors)
        return resp
