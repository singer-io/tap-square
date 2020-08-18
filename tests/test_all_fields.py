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

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.DYNAMIC)

        print("\n\nTESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.STATIC)

        self.set_environment(self.PRODUCTION)

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams().difference(self.sandbox_streams())
        self.all_fields_test(self.PRODUCTION, DataType.DYNAMIC)

    def all_fields_test(self, environment, data_type):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, force_create_records=True)

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
                    self.assertDictEqual(expected_record, actual_record)
