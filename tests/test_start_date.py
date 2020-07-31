import os
from datetime import datetime as dt
from datetime import timedelta

import unittest
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestSquareStartDate(TestSquareBase):
    START_DATE = ""
    START_DATE_1 = ""
    START_DATE_2 = ""

    def name(self):
        return "tap_tester_square_start_date_test"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'employees', # TODO Requires production environment to create records
                'modifier_lists',
                'inventories',
                'roles', #doesn't use start_date, this is a full table
                'settlements',
                'cash_drawer_shifts',
            }
        )

    def testable_streams_static(self):
        return self.static_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'bank_accounts', # Cannot create a record, also PROD ONLY
                'roles',
            }
        )

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING WITH DYNAMIC DATA")

        # Initialize start_date state to make assertions
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.START_DATE_1 = self.START_DATE
        self.START_DATE_2 = dt.strftime(dt.utcnow(), self.START_DATE_FORMAT)
        self.TESTABLE_STREAMS = self.testable_streams()
        self.start_date_test()

        # print("\n\nTESTING WITH STATIC DATA") # TODO no static streams currently testable
        # self.START_DATE = self.STATIC_START_DATE
        # self.START_DATE_1 = self.STATIC_START_DATE
        # self.START_DATE_2 = self.timedelta_formatted(self.STATIC_START_DATE, days=2)
        # self.TESTABLE_STREAMS = self.testable_streams_static()
        # self.start_date_test()

        # TODO implement PRODUCTION

    def start_date_test(self):
        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        # get expected records
        expected_records_1 = {}
        for stream in self.TESTABLE_STREAMS:
            existing_objects = self.client.get_all(stream, self.START_DATE)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            expected_records_1[stream] = existing_objects

            # If no objects exist since the 2nd start_date, create one
            data_in_range = False # TODO this can be cleaned up
            for obj in expected_records_1.get(stream):
                rep_keys = self.expected_replication_keys().get(stream)
                rep_key = list(rep_keys)[0] if rep_keys else None
                if rep_key is None:
                    key = obj.get('created_at')
                else:
                    key = obj.get(rep_key)
                if self.parse_date(key) > self.parse_date(self.START_DATE_2):
                    data_in_range = True
                    break
            if not data_in_range:
                if stream in self.TESTABLE_STREAMS:
                    expected_records_1[stream].append(self.client.create(stream), start_date=self.START_DATE)
                    continue
                assert None, "Sufficient test data does not exist for {}, test will fail.".format(stream)

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # Select all testable streams and their fields
        exclude_streams = self.expected_streams().difference(self.TESTABLE_STREAMS)
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=True, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        #clear state
        menagerie.set_state(conn_id, {})

        # Run sync 1
        sync_job_1 = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_1)

        # read target output
        record_count_by_stream_1 = runner.examine_target_output_file(self, conn_id,
                                                                     self.expected_streams(), self.expected_primary_keys())
        replicated_row_count_1 =  sum(record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

        state_1 = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        self.START_DATE = self.START_DATE_2
        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.START_DATE_1, self.START_DATE_2))

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id = connections.ensure_connection(self, original_properties=False)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # Select all available streams and their fields
        exclude_streams = self.expected_streams().difference(self.TESTABLE_STREAMS)
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=True, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # clear state
        menagerie.set_state(conn_id, {})

        # Run sync 2
        sync_job_2 = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_2)

        # This should be validating the the PKs are written in each record
        record_count_by_stream_2 = runner.examine_target_output_file(self, conn_id,
                                                                     self.expected_streams(), self.expected_primary_keys())
        replicated_row_count_2 =  sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))

        synced_records_2 = runner.get_records_from_target_output()

        state_2 = menagerie.get_state(conn_id)

        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)


                # Verify that the 2nd sync resutls in less records than the 1st sync.
                self.assertLessEqual(record_count_2, record_count_1,
                                msg="\nStream '{}' is {}\n".format(stream, self.FULL) +
                                "Second sync should result in fewer records\n" +
                                "Sync 1 start_date: {} ".format(self.START_DATE_1) +
                                "Sync 1 record_count: {}\n".format(record_count_1) +
                                "Sync 2 start_date: {} ".format(self.START_DATE_2) +
                                "Sync 2 record_count: {}".format(record_count_2))


                # Testing how FULL TABLE streams handle start date
                if replication_type == self.FULL:

                    # Verify that a bookmark doesn't exist for the stream.
                    self.assertTrue(state_1.get(stream) is None,
                                    msg="There should not be bookmark value for {}\n{}".format(stream, state_1.get(stream)))
                    self.assertTrue(state_2.get(stream) is None,
                                    msg="There should not be bookmark value for {}\n{}".format(stream, state_1.get(stream)))


                # Testing how INCREMENTAL streams handle start date
                elif replication_type == self.INCREMENTAL:

                    # Verify 1st sync record count > 2nd sync record count since the 1st start date is older than the 2nd.
                    self.assertGreater(replicated_row_count_1, replicated_row_count_2, msg="Expected less records on 2nd sync.")

                    # Verify all data from first sync has bookmark values >= start_date .
                    records_from_sync_1 = set(row.get('data').get('updated_at')
                                              for row in synced_records_1.get(stream, []).get('messages', []))
                    for record in records_from_sync_1:
                        self.assertGreaterEqual(self.parse_date(record), self.parse_date(self.START_DATE_1),
                                                msg="Record was created prior to start date for 1st sync.\n" +
                                                "Sync 1 start_date: {}\n".format(self.START_DATE_1) +
                                                "Record bookmark: {} ".format(record))

                    # Verify all data from second sync has bookmark values >= start_date 2.
                    records_from_sync_2 = set(row.get('data').get('updated_at')
                                              for row in synced_records_2.get(stream, {}).get('messages', []))
                    for record in records_from_sync_2:
                        self.assertGreaterEqual(self.parse_date(record), self.parse_date(self.START_DATE_2),
                                                msg="Record was created prior to start date for 2nd sync.\n" +
                                                "Sync 2 start_date: {}\n".format(self.START_DATE_2) +
                                                "Record bookmark: {} ".format(record))
                else:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))
