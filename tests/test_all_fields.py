import os
import unittest

import tap_tester.runner      as runner

from base import TestSquareBase, DataType


class TestSquareAllFields(TestSquareBase, unittest.TestCase):
    """Test that with all fields selected for a stream we replicate data as expected"""
    TESTABLE_STREAMS = set()

    @staticmethod
    def name():
        return "tap_tester_square_all_fields"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

    def ensure_dict_object(self, resp_object):
        """
        Ensure the response object is a dictionary and return it.
        If the object is a list, ensure the list contains only one dict item and return that.
        Otherwise fail the test.
        """
        if isinstance(resp_object, dict):
            return resp_object
        elif isinstance(resp_object, list):
            self.assertEqual(1, len(resp_object),
                             msg="Multiple objects were returned, but only 1 was expected")
            self.assertTrue(isinstance(resp_object[0], dict),
                            msg="Response object is a list of {} types".format(type(resp_object[0])))
            return resp_object[0]
        else:
            raise RuntimeError("Type {} was unexpected.\nRecord: {} ".format(type(resp_object), resp_object))

    def create_specific_payments(self):
        """Create a record using each source type, and a record that will autocomplete."""
        print("Creating a record using each source type, and the autocomplete flag.")
        payment_records = []
        descriptions = {  # (source type, autocomplete)
            ("card", False),
            ("card_on_file", False),
            ("gift_card", False),
            ("card", True),
        }
        for source_key, autocomplete in descriptions:
            payment_response = self.client._create_payment(autocomplete=autocomplete, source_key=source_key)
            payment_object = self.ensure_dict_object(payment_response)
            payment_records.append({'description': (source_key, autocomplete), 'record': payment_object})

        return payment_records

    def update_specific_payments(self, payments_to_update):
        """Perform specifc updates on specific payment records."""
        updated_records = []
        print("Updating payment records by completing, canceling and refunding them.")
        # Update a completed payment by making a refund (payments must have a status of 'COMPLETED' to process a refund)
        created_desc =  ("card", True)
        description = "refund"
        payment_to_update = [payment['record'] for payment in payments_to_update if payment['description'] == created_desc][0]
        _, payment_response = self.client.create_refund(self.START_DATE, payment_to_update)
        payment_object = self.ensure_dict_object(payment_response)
        updated_records.append({'description': description, 'record': payment_object})

        # Update a payment by completing it
        created_desc =  ("card_on_file", False)
        description = "complete"
        payment_to_update = [payment['record'] for payment in payments_to_update if payment['description'] == created_desc][0]
        payment_response = self.client._update_payment(payment_to_update.get('id'), obj=payment_to_update, action=description)
        payment_object = self.ensure_dict_object(payment_response)
        updated_records.append({'description': description, 'record': payment_object})

        # Update a payment by canceling it
        created_desc =  ("gift_card", False)
        description = "cancel"
        payment_to_update = [payment['record'] for payment in payments_to_update if payment['description'] == created_desc][0]
        payment_response = self.client._update_payment(payment_to_update.get('id'), obj=payment_to_update, action=description)
        payment_object = self.ensure_dict_object(payment_response)
        updated_records.append({'description': description, 'record': payment_object})

        return updated_records

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.DYNAMIC)

        print("\n\nTESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.STATIC)

        self.set_environment(self.PRODUCTION)

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams())
        self.all_fields_test(self.PRODUCTION, DataType.DYNAMIC)

    def all_fields_test(self, environment, data_type):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        # ensure data exists for sync streams and set expectations
        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, force_create_records=True)

        # Execute specific creates and updates for the payments stream in addition to the standard create
        if 'payments' in self.TESTABLE_STREAMS:

            created_payments = self.create_specific_payments()

            updated_payments = self.update_specific_payments(created_payments)

            print("Tracking fields found in created and updated payments.") # TODO move this to bottom?
            fields = set()
            for payments in [created_payments, updated_payments]:
                for payment in payments:
                    payment_fields = set(payment['record'].keys())
                    untracked_fields = payment_fields.difference(fields)
                    if untracked_fields:
                        print("Adding untracked fields to tracked set: {}".format(untracked_fields))
                        fields.update(payment_fields)

        # TODO delete if not needed (depends on use of created_payments and updated_payments in asertions/comparisons)
        # # modify data set to conform to expectations (json standards)
        # for stream, records in expected_records.items():
        #     print("Ensuring expected data for {} has values formatted correctly.".format(stream))
        #     self.modify_expected_records(records)

        (_, first_record_count_by_stream) = self.run_initial_sync(environment, data_type)

        replicated_row_count = sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Test by Stream
        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = set()
                for record in expected_records.get(stream):
                    expected_keys.update(record.keys())

                # Verify schema covers all fields
                schema_keys = set(self.expected_schema_keys(stream))
                self.assertEqual(
                    set(), expected_keys.difference(schema_keys),
                    msg="\nFields missing from schema: {}\n".format(expected_keys.difference(schema_keys))
                )

                # not a test, just logging the fields that are included in the schema but not in the expectations
                if schema_keys.difference(expected_keys):
                    print("WARNING Fields missing from expectations: {}".format(schema_keys.difference(expected_keys)))

                # Verify that all fields sent to the target fall into the expected schema
                for actual_keys in record_messages_keys:
                    self.assertTrue(
                        actual_keys.issubset(schema_keys),
                        msg="Expected all fields to be present, as defined by schemas/{}.json".format(stream) +
                        "EXPECTED (SCHEMA): {}\nACTUAL (REPLICATED KEYS): {}".format(schema_keys, actual_keys))

                actual_records = [row['data'] for row in data['messages']]

                (expected_pks_to_record_dict, actual_pks_to_record_dict) = self.assertRecordsEqualByPK(stream, expected_records.get(stream), actual_records)

                for pks_tuple, expected_record in expected_pks_to_record_dict.items():
                    actual_record = actual_pks_to_record_dict.get(pks_tuple)
                    self.assertRecordsEqual(stream, expected_record, actual_record)
