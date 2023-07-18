import os

import tap_tester.connections as connections
import tap_tester.runner      as runner

from base import TestSquareBaseParent, DataType


class TestAutomaticFields(TestSquareBaseParent.TestSquareBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_square_automatic_fields"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams()).difference({
            'inventories', # No PK or rep key so no automatic fields to check
        })

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams()) - {'customers', 'team_members'}
        self.auto_fields_test(self.SANDBOX, DataType.DYNAMIC)

        print("\n\nTESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams()) - {'customers', 'team_members'}
        self.auto_fields_test(self.SANDBOX, DataType.STATIC)

        # self.set_environment(self.PRODUCTION)

        # print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        # self.START_DATE = self.get_properties().get('start_date')
        # self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams())
        # self.auto_fields_test(self.PRODUCTION, DataType.DYNAMIC)

    def auto_fields_test(self, environment, data_type):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        # ensure data exists and set expectatinos with automatic fields only
        expected_records_all_fields = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE)
        expected_records = {x: [] for x in self.expected_streams()}
        for stream in self.TESTABLE_STREAMS:
            existing_objects = expected_records_all_fields.get(stream)
            for obj in existing_objects:
                expected_records[stream].append(
                    {field: obj.get(field)
                     for field in self.expected_automatic_fields().get(stream)}
                )

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        streams_to_select = self.testable_streams(environment, data_type)
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=False
        )

        # run initial sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)

        replicated_row_count = sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))

        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(set(),
                                     actual_keys.symmetric_difference(expected_keys),
                                     msg="Expected automatic fields and nothing else.")

                actual_records = [row['data'] for row in data['messages']]
                self.assertPKsEqual(stream, expected_records.get(stream), actual_records)

                expected_pks_to_record_dict = self.getPKsToRecordsDict(stream, expected_records.get(stream))
                actual_pks_to_record_dict = self.getPKsToRecordsDict(stream, actual_records)

                for pks_tuple, expected_record in expected_pks_to_record_dict.items():
                    actual_record = actual_pks_to_record_dict.get(pks_tuple)
                    self.assertDictEqual(expected_record, actual_record)
