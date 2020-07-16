import unittest
import simplejson

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestSquareIncrementalReplication(TestSquareBase):

    def name(self):
        return "tap_tester_square_incremental_replication"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS NOT CURRENTY TESTABLE
                'employees', # Requires production environment to create records
                'refunds',
                'payments',
                'modifier_lists'
            }
        )
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
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Instatiate default start date
        self.START_DATE = self.get_properties().get('start_date')

        # Ensure tested streams have existing records
        expected_records_1 = {x: [] for x in self.expected_streams()}
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream, self.START_DATE)

            if len(existing_objects) == 0:
                # TODO change from 'failure you need data' to 'create data then so we can test'
                assert None, "NO DATA EXISTS FOR {}, SOMETHING HAS GONE TERRIBLY WRONG".format(stream)

            expected_records_1[stream] += existing_objects
            print('{}: Have sufficent amount of data to continue test'.format(stream))

        # Adjust expectations for datetime format
        for stream, expected_records in expected_records_1.items():
            print("Adjust expectations for stream: {}".format(stream))
            self.modify_expected_records(expected_records)

        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all testable streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_to_select = self.testable_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), streams_to_select,
                         msg="Expect first_sync_record_count keys {} to equal testable streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             streams_to_select,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Add data before next sync via insert and update, and set expectations
        created_records = {x: [] for x in self.expected_streams()}
        updated_records = {x: [] for x in self.expected_streams()}
        expected_records_2 = {x: [] for x in self.expected_streams()}
        for stream in self.testable_streams():
            # Create
            new_record = self.client.create(stream)
            assert len(new_record) > 0, "Failed to create a {} record".format(stream)
            assert len(new_record) == 1, "Created too many {} records: {}".format(stream, len(new_record))
            expected_records_2[stream] += new_record
            created_records[stream] = new_record.pop()
            # Update
            first_rec = first_sync_records.get(stream).get('messages')[0].get('data')
            first_rec_id = first_rec.get('id')
            first_rec_version = first_rec.get('version')
            updated_record = self.client.update(stream, first_rec_id, first_rec_version)
            assert len(updated_record) > 0, "Failed to update a {} record".format(stream)
            assert len(updated_record) == 1, "Updated too many {} records".format(stream)
            expected_records_2[stream] += updated_record
            updated_records[stream] = updated_record.pop()

        # adjust expectations for full table streams to include the expected records from sync 1
        for stream in self.expected_full_table_streams():
            for record in expected_records_1.get(stream, []):
                if record.get('id') == updated_records[stream].get('id'):
                    continue  # do not add the orginal of the updated record
                expected_records_2[stream].append(record)

        # Adjust expectations for datetime format
        for record_set in [created_records, updated_records]:
            for stream, expected_records in record_set.items():
                print("Adjust expectations for stream: {}".format(stream))
                self.modify_expected_record(expected_records)

        for stream, expected_records in expected_records_2.items():
                print("Adjust expectations for stream: {}".format(stream))
                self.modify_expected_records(expected_records)

        # ensure validity of expected_records_2
        for stream in self.testable_streams():
            if stream in self.expected_incremental_streams():
                assert len(expected_records_2.get(stream)) == 2, "Expectations are invalid for" + \
                    " incremental stream {}".format(stream)
            if stream in self.expected_full_table_streams():
                assert len(expected_records_2.get(stream)) ==  len(expected_records_1.get(stream)) + 1, "Expectations are " + \
                    "invalid for full table stream {}".format(stream)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)

        # Loop first_sync_records and compare against second_sync_records
        for stream in self.testable_streams():
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

                    # verify that you get less/same amount of data on the 2nd sync
                    self.assertGreater(
                        first_sync_record_count.get(stream, 0),
                        second_sync_record_count.get(stream, 0),
                        msg="first sync didn't have more records, bookmark usage not verified")

                    # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                    replication_key = next(iter(self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)))
                    first_sync_bookmark = first_sync_state.get('bookmarks').get(stream).get(replication_key)
                    for record in second_sync_data:
                        date_value = record["updated_at"]
                        self.assertGreater(date_value,
                                           first_sync_bookmark,
                                           msg="First sync bookmark is not less than 2nd sync record's replication-key")

                elif stream in self.expected_full_table_streams():

                    # TESTING FULL TABLE STREAMS

                    # Verify no bookmarks are present
                    first_state = first_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual(first_state, {},
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(first_sync_state) + \
                                     "\tBookmark: {}".format(first_state))
                    second_state = second_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual(second_state, {},
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(second_sync_state) + \
                                     "\tBookmark: {}".format(second_state))

                # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see only 2 records
                # For full table streams we should see 1 more record than the first sync
                expected_records = expected_records_2.get(stream)
                self.assertEqual(len(expected_records), len(second_sync_data),
                                 msg="Expected number of records do not match actual for 2nd sync.\n" +
                                 "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                )

                # Verify that the inserted records are replicated by the 2nd sync and match our expectations
                schema_keys = set(self.expected_schema_keys(stream))
                created_record = created_records.get(stream)
                updated_record = updated_records.get(stream)
                if not created_record in second_sync_data:
                    print("\n\nDATA DISCREPANCY FOR CREATED RECORD: {}\n".format(stream))
                    print("EXPECTED RECORD: {}\n".format(created_record))
                    replicated_created_record = [record for record in second_sync_data
                                                 if created_record.get('id') == record.get('id')]
                    print("ACTUAL RECORD: {}".format(replicated_created_record))
                    for key in schema_keys:
                        val = replicated_created_record[0].get(key)
                        e_val = created_record.get(key)
                        if e_val != val:
                            print("\nDISCREPANCEY | KEY {}: ACTUAL: {} EXPECTED {}".format(key, val, e_val))
                # BUG | https://stitchdata.atlassian.net/browse/SRCE-3532
                self.assertIn(created_record, second_sync_data,
                              msg="Data discrepancy for the created record.")

                # Verify that the updated records are replicated by the 2nd sync and match our expectations
                if not updated_record in second_sync_data:
                    print("\n\nDATA DISCREPANCY FOR UPDATED RECORD: {}\n".format(stream))
                    print("EXPECTED RECORD: {}\n".format(updated_record))
                    replicated_updated_record = [record for record in second_sync_data
                                                 if updated_record.get('id') == record.get('id')]
                    print("ACTUAL RECORD: {}".format(replicated_updated_record[0]))
                    for key in schema_keys:
                        val = replicated_updated_record[0].get(key)
                        e_val = updated_record.get(key)
                        if e_val != val:
                            print("\nDISCREPANCEY | KEY {}: ACTUAL: {} EXPECTED {}".format(key, val, e_val))
                self.assertIn(updated_record, second_sync_data,
                              msg="Data discrepancy for the updated record.")


if __name__ == '__main__':
    unittest.main()
