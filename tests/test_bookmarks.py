import unittest
import simplejson

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestSquareIncrementalReplication(TestSquareBase):
    START_DATE = "2020-06-01T00:00:00Z"

    def get_properties(self, original = True):
        return_value = {
            'start_date' : '2020-06-01T00:00:00Z',
            'sandbox' : 'true'
        }

        if original:
            return return_value

        return_value['start_date'] = self.START_DATE
        return return_value

    def name(self):
        return "tap_tester_square_incremental_replication"

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
        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        # incremental_streams = {key for key, value in self.expected_replication_method().items()
        #                        if value == self.INCREMENTAL}
        incremental_streams = self.expected_incremental_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), incremental_streams,
                         msg="Expect first_sync_record_count keys {} to equal incremental_streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             incremental_streams,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Add data before next sync via insert and update
        for stream in incremental_streams:
            self.client.create(stream)
            first_rec_id = first_sync_records[stream]['messages'][0]['data']['id']
            first_rec_version = first_sync_records[stream]['messages'][0]['data']['version']
            self.client.update(stream, first_rec_id, first_rec_version)


        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)

        # Loop first_sync_records and compare against second_sync_records
        for stream in incremental_streams:
            with self.subTest(stream=stream):

                # Verify both syncs write / keep the same bookmark
                self.assertEqual(set(first_sync_state['bookmarks'].keys()),
                                 set(second_sync_state['bookmarks'].keys()))

                # Verify second sync's bookmarks move past the first sync's
                self.assertGreater(second_sync_state['bookmarks'][stream]['updated_at'],
                                   first_sync_state['bookmarks'][stream]['updated_at'])


                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   msg="Data isn't set up to be able to test full sync")

                # verify that you get less/same amount of data on the 2nd sync
                self.assertGreaterEqual(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="first sync didn't have more records, bookmark usage not verified")

                # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                second_data = [record["data"]["updated_at"] for record
                               in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                replication_key = next(iter(self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)))
                first_sync_bookmark = first_sync_state.get('bookmarks').get(stream).get(replication_key)
                for date_value in second_data:

                    self.assertGreater(date_value,
                                     first_sync_bookmark,
                                     msg="First sync bookmark is not less than 2nd sync record's replication-key")
