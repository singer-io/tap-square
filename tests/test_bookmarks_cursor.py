import singer

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBaseParent

LOGGER = singer.get_logger()


class TestSquareIncrementalReplicationCursor(TestSquareBaseParent.TestSquareBase):

    def testable_streams_dynamic(self):
        all_testable_streams = self.dynamic_data_streams().intersection(
            self.expected_full_table_streams()).difference(
                self.untestable_streams())

        # Shifts have cursor bookmarks because the api doesn't
        # support incremental queries, but we fake it being
        # incremental
        all_testable_streams.add('shifts')

        return all_testable_streams

    @staticmethod
    def testable_streams_static():
        """ No static streams contain cursor bookmarks. """
        return set()

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.START_DATE = self.get_properties().get('start_date')

        self.bookmarks_test(self.testable_streams_dynamic().intersection(self.sandbox_streams()))

        TestSquareBaseParent.TestSquareBase.test_name = self.prod_test_name
        self.set_environment(self.PRODUCTION)
        production_testable_streams = self.testable_streams_dynamic().intersection(self.production_streams())
        if production_testable_streams:
            self.bookmarks_test(production_testable_streams)

        TestSquareBaseParent.TestSquareBase.test_name = self.sandbox_test_name

    def bookmarks_test(self, testable_streams):
        """
        Verify for each stream that you can do a sync which records bookmark cursor
        Verify that the number of records in the sync is equal to all expected records minus the first page

        PREREQUISITE
        For EACH stream that is interruptable with a bookmark cursor and not another one is replicated there are more than 1 page of data
        """
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Ensure tested streams have existing records
        stream_to_expected_records_before_removing_first_page = self.create_test_data(testable_streams, self.START_DATE, min_required_num_records_per_stream=self.API_LIMIT)


        # verify the expected test data exceeds API LIMIT for all testable streams
        for stream in testable_streams:
            record_count = len(stream_to_expected_records_before_removing_first_page[stream])
            print("Verifying data is sufficient for stream {}. ".format(stream) +
                  "\tRecord Count: {}\tAPI Limit: {} ".format(record_count, self.API_LIMIT.get(stream)))
            self.assertGreater(record_count, self.API_LIMIT.get(stream),
                               msg="Pagination not ensured.\n" +
                               "{} does not have sufficient data in expecatations.\n ".format(stream))

        stream_to_first_page_records = dict()
        stream_to_cursor = dict()
        stream_to_expected_records = dict()
        for testable_stream in testable_streams:
            stream_to_first_page_records[testable_stream], stream_to_cursor[testable_stream] = self.client.get_first_page_and_cursor(testable_stream, self.START_DATE)
            first_page_pks_to_records = self.getPKsToRecordsDict(testable_stream, stream_to_first_page_records[testable_stream])
            all_pks_to_records = self.getPKsToRecordsDict(testable_stream, stream_to_expected_records_before_removing_first_page[testable_stream])
            stream_to_expected_records[testable_stream] = [
                record for pk, record in all_pks_to_records.items()
                if pk not in first_page_pks_to_records
            ]

        # Create connection but do not use default start date
        conn_id = connections.ensure_connection(self, original_properties=False, payload_hook=self.preserve_access_token)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select=testable_streams, select_all_fields=True
        )

        bookmarks = {
            testable_stream: {
                "cursor": stream_to_cursor[testable_stream]
            }
            for testable_stream in testable_streams
        }
        menagerie.set_state(conn_id, {"bookmarks": bookmarks})

        # run initial sync
        self.run_and_verify_sync(conn_id, clear_state=False)

        synced_records = runner.get_records_from_target_output()
        for stream in testable_streams:
            with self.subTest(stream=stream):
                data = synced_records.get(stream, [])
                actual_records = [row['data'] for row in data['messages']]
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                auto_fields = self.expected_automatic_fields().get(stream)

                for actual_keys in record_messages_keys:

                    # Verify that the automatic fields are sent to the target for paginated streams
                    self.assertTrue(auto_fields.issubset(actual_keys),
                                    msg="A paginated synced stream has a record that is missing automatic fields.")

                    # Verify we have more fields sent to the target than just automatic fields (this is set above)
                    self.assertEqual(set(), auto_fields.difference(actual_keys),
                                     msg="A paginated synced stream has a record that is missing expected fields.")

                # Verify by pks that the replicated records match our expectations
                self.assertPKsEqual(stream, stream_to_expected_records.get(stream), actual_records, assert_pk_count_same=True)
