import os

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import singer
from unittest import TestCase
from base import TestSquareBase

LOGGER = singer.get_logger()


class TestSquarePagination(TestSquareBase, TestCase):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""

    DEFAULT_BATCH_LIMIT = 1000
    API_LIMIT = {
        'items': DEFAULT_BATCH_LIMIT,
        'inventories': DEFAULT_BATCH_LIMIT,
        'categories': DEFAULT_BATCH_LIMIT,
        'discounts': DEFAULT_BATCH_LIMIT,
        'taxes': DEFAULT_BATCH_LIMIT,
        'cash_drawer_shifts': DEFAULT_BATCH_LIMIT,
        'employees': 50,
        'locations': None, # Api does not accept a cursor and documents no limit, see https://developer.squareup.com/reference/square/locations/list-locations
        'roles': 100,
        'refunds': 100,
        'payments': 100,
        'modifier_lists': DEFAULT_BATCH_LIMIT,
        'orders': 500,
        'shifts': 200,
        'settlements': 200,
    }

    def name(self):
        return "tap_tester_square_pagination_test"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams()).difference({
            'locations',  # This stream does not paginate in the sync (See Above)
        })

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams())
        self.pagination_test()

        print("\n\n-- SKIPPING -- TESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.assertEqual(set(), self.TESTABLE_STREAMS,
                         msg="Testable streams exist for this category.")
        print("\tThere are no testable streams.")

        self.set_environment(self.PRODUCTION)

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams())
        self.pagination_test()

        print("\n\n-- SKIPPING -- TESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.sandbox_streams())
        self.assertEqual(set(), self.TESTABLE_STREAMS,
                         msg="Testable streams exist for this category.")
        print("\tThere are no testable streams.")

    def pagination_test(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, min_required_num_records_per_stream=self.API_LIMIT)

        # verify the expected test data exceeds API LIMIT for all testable streams
        for stream in self.TESTABLE_STREAMS:
            record_count = len(expected_records[stream])
            print("Verifying data is sufficient for stream {}. ".format(stream) +
                  "\tRecord Count: {}\tAPI Limit: {} ".format(record_count, self.API_LIMIT.get(stream)))
            self.assertGreater(record_count, self.API_LIMIT.get(stream),
                               msg="Pagination not ensured.\n" +
                               "{} does not have sufficient data in expecatations.\n ".format(stream))

        # Create connection but do not use default start date
        conn_id = connections.ensure_connection(self, original_properties=False)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(0, len(diff), msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        #select all catalogs
        exclude_streams = list(self.expected_streams().difference(self.TESTABLE_STREAMS))
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs,
                                           exclude_streams=exclude_streams)

        #clear state
        version = menagerie.get_state_version(conn_id)
        menagerie.set_state(conn_id, {}, version)

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                   self.TESTABLE_STREAMS,
                                                                   self.expected_primary_keys())
        synced_records = runner.get_records_from_target_output()
        for stream in self.TESTABLE_STREAMS:
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

                primary_keys = self.expected_primary_keys().get(stream)
                pks = list(primary_keys) if primary_keys else None
                if not pks:
                    pks = self.makeshift_primary_keys().get(stream)

                # Verify by pks that the replicated records match our expectations
                self.assertRecordsEqualByPK(stream, expected_records.get(stream), actual_records, pks)

                # Verify the expected number of records were replicated
                self.assertEqual(len(expected_records.get(stream)), len(actual_records))


    def assertRecordsEqualByPK(self, stream, expected_records, actual_records, pks):
        """Compare expected and actual records by their primary key."""

        # Verify there are no duplicate pks in the target
        actual_pks = [tuple(actual_record.get(pk) for pk in pks) for actual_record in actual_records]
        actual_pks_set = set(actual_pks)
        self.assertEqual(len(actual_pks), len(actual_pks_set), msg="A duplicate record may have been replicated.")

        # Verify there are no duplicate pks in our expectations
        expected_pks = [tuple(expected_record.get(pk) for pk in pks) for expected_record in expected_records]
        expected_pks_set = set(expected_pks)
        self.assertEqual(len(expected_pks), len(expected_pks_set), msg="Our expectations contain a duplicate record.")

        # Verify that all expected records and ONLY the expected records were replicated
        self.assertEqual(expected_pks_set, actual_pks_set)


if __name__ == '__main__':
    unittest.main()
