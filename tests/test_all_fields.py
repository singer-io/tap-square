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

        # ensure data exists for sync streams and set expectations
        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE)
        # modify data set to conform to expectations (json standards)
        for stream, records in expected_records.items():
            print("Ensuring expected data for {} has values formatted correctly.".format(stream))
            self.modify_expected_records(records)

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
                expected_keys = list(expected_records.get(stream)[0].keys())
                primary_keys = self.expected_primary_keys().get(stream)
                pk = list(primary_keys)[0] if primary_keys else None

                # Verify schema covers all fields
                schema_keys = set(self.expected_schema_keys(stream))
                self.assertEqual(
                    set(), set(expected_keys).difference(schema_keys),
                    msg="\nFields missing from schema: {}\n".format(set(expected_keys).difference(schema_keys))
                )

                # not a test, just logging the fields that are included in the schema but not in the expectations
                if schema_keys.difference(set(expected_keys)):
                    print("WARNING Fields missing from expectations: {}".format(schema_keys.difference(set(expected_keys))))

                # Verify that all fields sent to the target fall into the expected schema
                for actual_keys in record_messages_keys:
                    self.assertTrue(
                        actual_keys.issubset(schema_keys),
                        msg="Expected all fields to be present, as defined by schemas/{}.json".format(stream) +
                        "EXPECTED (SCHEMA): {}\nACTUAL (REPLICATED KEYS): {}".format(schema_keys, actual_keys))

                actual_records = [row['data'] for row in data['messages']]

                if pk:

                    stream_expected_record_ids = {record[pk] for record in expected_records.get(stream)}
                    stream_actual_record_ids = {record[pk] for record in actual_records}
                    self.assertEqual(stream_expected_record_ids,
                                     stream_actual_record_ids)

                    # Test by keys and values, that we replicated the expected records and nothing else

                    # Verify that actual records were in our expectations
                    for actual_record in actual_records:
                        stream_expected_records = [record for record in expected_records.get(stream)
                                                   if actual_record.get(pk) == record.get(pk)]
                        self.assertTrue(len(stream_expected_records),
                                        msg="An actual record is missing from our expectations: \nRECORD: {}".format(actual_record))
                        self.assertEqual(1, len(stream_expected_records),
                                         msg="A duplicate record was found in our expectations for {}.".format(stream))
                        stream_expected_record = stream_expected_records[0]
                        self.assertDictEqual(stream_expected_record, actual_record)

                    # Verify that our expected records were replicated by the tap
                    for expected_record in expected_records.get(stream):
                        stream_actual_records = [record for record in actual_records
                                                 if expected_record.get(pk) == record.get(pk)]
                        self.assertTrue(len(stream_actual_records),
                                        msg="An expected record is missing from the sync: \nRECORD: {}".format(expected_record))
                        self.assertEqual(1, len(stream_actual_records),
                                         msg="A duplicate record was found in the sync for {}.".format(stream))
                        stream_actual_record = stream_actual_records[0]
                        self.assertDictEqual(expected_record, stream_actual_record)

                else:  # 'inventories' does not have a pk so our assertions aren't as clean

                    # Verify that actual records were in our expectations
                    for actual_record in actual_records:
                        if actual_record not in expected_records.get(stream):
                            print("DATA DISCREPANCY:\n\nACTUAL RECORD:\n{}\n".format(actual_record))
                            for record in expected_records.get(stream):
                                if record.get('catalog_object_id') == actual_record.get('catalog_object_id') and \
                                   record.get('location_id') == actual_record.get('location_id'):
                                    print("EXPECTED_RECORDS:")
                                    print(str(record))
                        self.assertIn(actual_record, expected_records.get(stream))

                    # Verify that our expected records were replicated by the tap
                    for expected_record in expected_records.get(stream):
                        if expected_record not in actual_records:
                            print("DATA DISCREPANCY:\n\nEXPECTED RECORD:\n{}\n".format(expected_record))
                            for record in actual_records:
                                if record.get('catalog_object_id') == expected_record.get('catalog_object_id') and \
                                   record.get('location_id') == expected_record.get('location_id'):
                                    print("ACTUAL_RECORDS:")
                                    print(str(record))
                        self.assertIn(expected_record, actual_records)

                    self.assertEqual(len(expected_records.get(stream)), len(actual_records),
                                     msg="Unexpected number of records synced.")


if __name__ == '__main__':
    unittest.main()
