import os
from datetime import datetime as dt
from datetime import timedelta

import singer

import tap_tester.connections as connections
import tap_tester.runner      as runner

from base import TestSquareBaseParent, DataType

LOGGER = singer.get_logger()


class TestSquareStartDate(TestSquareBaseParent.TestSquareBase):
    START_DATE = ""
    START_DATE_1 = ""
    START_DATE_2 = ""

    @staticmethod
    def name():
        return "tap_tester_square_start_date_test"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams()).difference({
            'locations', # As discussed with PM, because this is a parent stream of a few other streams intentionally does not respect start_date
        })

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')  # Initialize start_date state to make assertions
        self.START_DATE_1 = self.START_DATE
        self.START_DATE_2 = dt.strftime(dt.utcnow(), self.START_DATE_FORMAT)
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams())
        self.start_date_test(self.get_environment(), DataType.DYNAMIC)

        # Locations does not respect start date and it's the only static data type (see above)
        print("\n\n-- SKIPPING -- TESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.assertEqual(set(), self.TESTABLE_STREAMS,
                         msg="Testable streams exist for this category.")
        print("\tThere are no testable streams.")

        self.set_environment(self.PRODUCTION)

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.START_DATE_1 = self.START_DATE
        self.START_DATE_2 = dt.strftime(dt.utcnow(), self.START_DATE_FORMAT)
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams()) - {'customers', 'inventories', 'items'}
        self.start_date_test(self.get_environment(), DataType.DYNAMIC)

    def start_date_test(self, environment, data_type):
        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE_1, self.START_DATE_2)

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        streams_to_select = self.testable_streams(environment, data_type)
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=True
        )

        # run initial sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)

        replicated_row_count_1 = sum(first_record_count_by_stream.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(first_record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

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

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        streams_to_select = self.testable_streams(environment, data_type)
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=True
        )

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id)

        replicated_row_count_2 = sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))

        synced_records_2 = runner.get_records_from_target_output()

        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)
                record_count_1 = first_record_count_by_stream.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Verify that the 2nd sync resutls in less records than the 1st sync.
                self.assertLessEqual(record_count_2, record_count_1,
                                     msg="\nStream '{}' is {}\n".format(stream, self.FULL) +
                                     "Second sync should result in fewer records\n" +
                                     "Sync 1 start_date: {} ".format(self.START_DATE_1) +
                                     "Sync 1 record_count: {}\n".format(record_count_1) +
                                     "Sync 2 start_date: {} ".format(self.START_DATE_2) +
                                     "Sync 2 record_count: {}".format(record_count_2))

                # Verify 1st sync record count > 2nd sync record count for incremental streams
                if replication_type == self.INCREMENTAL:
                    self.assertGreater(replicated_row_count_1, replicated_row_count_2, msg="Expected less records on 2nd sync.")

                elif replication_type != self.FULL:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))

                self.assertRecordsDateGreaterEqual(synced_records_1, stream, self.START_DATE_1)
                self.assertRecordsDateGreaterEqual(synced_records_2, stream, self.START_DATE_2)

    def assertRecordsDateGreaterEqual(self, sync_records, stream, start_date):
        records_date_data = set(
            row.get('data').get(self.get_start_date_key(stream))
            for row in sync_records.get(stream, []).get('messages', [])
        )
        for record_date in records_date_data:
            self.assertGreaterEqual(
                self.parse_date(record_date), self.parse_date(start_date),
                msg="Record was created prior to start date for this sync.\n" +
                "Sync start_date: {}\n".format(start_date) +
                "Record date: {} ".format(record_date)
            )
