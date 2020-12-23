import os

import singer

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBaseParent

LOGGER = singer.get_logger()


class TestSquareIncrementalReplicationCursor(TestSquareBaseParent.TestSquareBase):

    @staticmethod
    def name():
        return "tap_tester_square_incremental_replication_cursor"

    def testable_streams_dynamic(self):
        return {"inventories"}
        # return self.dynamic_data_streams().intersection(
        #     self.expected_full_table_streams()).difference(
        #         self.untestable_streams())

    @staticmethod
    def testable_streams_static():
        """ No static streams marked for incremental. """
        return set()

    @staticmethod
    def cannot_update_streams():
        return {
            'refunds',  # Does not have an endpoint for updating records
            'modifier_lists',  # Has endpoint but just adds/removes mod_list from an item.
        }

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        return sync_record_count

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.START_DATE = self.get_properties().get('start_date')

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.bookmarks_test(self.testable_streams_dynamic().intersection(self.sandbox_streams()))

        self.set_environment(self.PRODUCTION)
        production_testable_streams = self.testable_streams_dynamic().intersection(self.production_streams())
        if production_testable_streams:
            print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
            self.bookmarks_test(production_testable_streams)

    def bookmarks_test(self, testable_streams):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is the max value sent to the target for the `date` PK field
        Verify that the 2nd sync respects the bookmark
        Verify that all data of the 2nd sync is >= the bookmark from the first sync
        Verify that the number of records in the 2nd sync is less then the first
        Verify inclusivivity of bookmarks

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
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
        conn_id = connections.ensure_connection(self, original_properties=False)

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
        record_count_by_stream = self.run_and_verify_sync(conn_id, clear_state=False)

        synced_records = runner.get_records_from_target_output()
        for stream in testable_streams:
            with self.subTest(stream=stream):
                # Verify we are paginating for testable synced streams
                self.assertGreater(record_count_by_stream.get(stream, -1), self.API_LIMIT.get(stream),
                                   msg="We didn't guarantee pagination. The number of records should exceed the api limit.")

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
                self.assertPKsEqual(stream, stream_to_expected_records.get(stream), actual_records)
