import os

from unittest import TestCase

import tap_tester.runner      as runner

from base import TestSquareBase, DataType


class TestAutomaticFields(TestSquareBase, TestCase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    def name(self):
        return "tap_tester_square_automatic_fields"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

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


    def auto_fields_test(self, environment, data_type):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        expected_records_all_fields = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, force_create_records=True)
        expected_records = {x: [] for x in self.expected_streams()}
        for stream in self.TESTABLE_STREAMS:
            existing_objects = expected_records_all_fields.get(stream)
            for obj in existing_objects:
                expected_records[stream].append(
                    {field: obj.get(field)
                     for field in self.expected_automatic_fields().get(stream)}
                )

        (_, first_record_count_by_stream) = self.run_initial_sync(environment, data_type)

        replicated_row_count =  sum(first_record_count_by_stream.values())
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
                expected_keys = self.expected_automatic_fields().get(stream)
                primary_keys = self.expected_primary_keys().get(stream)
                pk = list(primary_keys)[0] if primary_keys else None

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(set(),
                                     actual_keys.symmetric_difference(expected_keys),
                                     msg="Expected automatic fields and nothing else.")

                actual_records = [row['data'] for row in data['messages']]

                # Verify the number of records match expectations
                self.assertEqual(len(expected_records.get(stream)),
                                 len(actual_records),
                                 msg="Number of actual records do match expectations. " +\
                                 "We probably have duplicate records.")

                # Test by keys and values, that we replicated the expected records and nothing else

                if pk:  # If the stream has a pk we can compare exact records

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
                        self.assertIn(actual_record, expected_records.get(stream))

                    # Verify that our expected records were replicated by the tap
                    for expected_record in expected_records.get(stream):
                        self.assertIn(expected_record, actual_records)
