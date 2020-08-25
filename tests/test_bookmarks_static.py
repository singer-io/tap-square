import os
import unittest

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBaseParent


class TestSquareIncrementalReplication(TestSquareBaseParent.TestSquareBase):
    STATIC_START_DATE = "2020-07-13T00:00:00Z"

    def name(self):
        return "tap_tester_square_incremental_replication"

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

    def testable_streams_dynamic(self):
        return set()

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
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Instatiate static start date
        self.START_DATE = self.STATIC_START_DATE

        # Ensure tested streams have data
        expected_records_first_sync = self.create_test_data(self.testable_streams_static(), self.START_DATE)

        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all testable streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_to_select = self.testable_streams_static()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(streams_to_select, set(first_sync_record_count.keys()),
                         msg="Expect first_sync_record_count keys {} to equal testable streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             streams_to_select,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Set expectations for 2nd sync
        expected_records_second_sync = {x: [] for x in self.expected_streams()}
        # adjust expectations for full table streams to include the expected records from sync 1
        for stream in self.testable_streams_static():
            if stream in self.expected_full_table_streams():
                for record in expected_records_first_sync.get(stream, []):
                    expected_records_second_sync[stream].append(record)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)

        # Loop first_sync_records and compare against second_sync_records
        for stream in self.testable_streams_static():
            with self.subTest(stream=stream):

                second_sync_data = [record.get("data") for record
                                    in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                # TESTING INCREMENTAL STREAMS
                if stream in self.expected_incremental_streams():

                    # Verify both syncs write / keep the same bookmark
                    self.assertEqual(set(first_sync_state.get('bookmarks', {}).keys()),
                                     set(second_sync_state.get('bookmarks', {}).keys()))

                    # Verify second sync's bookmarks move past the first sync's
                    self.assertGreater(
                        second_sync_state.get('bookmarks', {stream: {}}).get(stream, {'updated_at': -1}).get('updated_at'),
                        first_sync_state.get('bookmarks', {stream: {}}).get(stream, {'updated_at': -1}).get('updated_at')
                    )

                    # verify that there is more than 1 record of data - setup necessary
                    self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                       msg="Data isn't set up to be able to test full sync")

                    # verify that you get no data on the 2nd sync
                    self.assertGreaterEqual(0, second_sync_record_count.get(stream, 0),
                                            msg="first sync didn't have more records, bookmark usage not verified")

                elif stream in self.expected_full_table_streams():

                    # TESTING FULL TABLE STREAMS

                    # Verify no bookmarks are present
                    first_state = first_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual({}, first_state,
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(first_sync_state) + \
                                     "\tBookmark: {}".format(first_state))
                    second_state = second_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual({}, second_state,
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(second_sync_state) + \
                                     "\tBookmark: {}".format(second_state))

               # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see 0 records
                # For full table streams we should see the same records from the first sync
                expected_records = expected_records_second_sync.get(stream, [])
                self.assertEqual(len(expected_records), len(second_sync_data),
                                 msg="Expected number of records do not match actual for 2nd sync.\n" +
                                 "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                )


if __name__ == '__main__':
    unittest.main()
