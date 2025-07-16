import os

import singer
import tap_tester.connections as connections
import tap_tester.runner      as runner

from base import TestSquareBaseParent
from test_client import TestClient

LOGGER = singer.get_logger()



class TestSquarePagination(TestSquareBaseParent.TestSquareBase):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""


    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams()).difference({
            'locations',  # This stream does not paginate in the sync (See Above)
        })

    @classmethod
    def tearDownClass(cls):
        cls.set_environment(cls(), cls.SANDBOX)
        cleanup = {'categories': 10000}
        client = TestClient(env=os.environ['TAP_SQUARE_ENVIRONMENT'])
        for stream, limit in cleanup.items():
            LOGGER.info('Checking if cleanup is required.')
            all_records = client.get_all(stream, start_date=cls.STATIC_START_DATE)
            all_ids = [rec.get('id') for rec in all_records if not rec.get('is_deleted')]
            if len(all_ids) > limit / 2:
                chunk = int(len(all_ids) - (limit / 2))
                LOGGER.info('Cleaning up {} excess records'.format(chunk))
                client.delete_catalog(all_ids[:chunk])

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        LOGGER.info('\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams())
        self.pagination_test()

        LOGGER.info('\n\n-- SKIPPING -- TESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.assertEqual(set(), self.TESTABLE_STREAMS,
                         msg="Testable streams exist for this category.")
        LOGGER.info('\tThere are no testable streams.')

        TestSquareBaseParent.TestSquareBase.test_name = self.TEST_NAME_PROD
        self.set_environment(self.PRODUCTION)

        LOGGER.info('\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams())
        self.pagination_test()
        TestSquarePagination.test_name = "tap_tester_sandbox_square_pagination_test"

        LOGGER.info('\n\n-- SKIPPING -- TESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.sandbox_streams())
        self.assertEqual(set(), self.TESTABLE_STREAMS,
                         msg="Testable streams exist for this category.")
        LOGGER.info('\tThere are no testable streams.')
        TestSquareBaseParent.TestSquareBase.test_name = self.TEST_NAME_SANDBOX

    def pagination_test(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        LOGGER.info('\n\nRUNNING {}_pagination'.format(self.name()))
        LOGGER.info('WITH STREAMS: {}\n\n'.format(self.TESTABLE_STREAMS))

        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, min_required_num_records_per_stream=self.API_LIMIT)

        # verify the expected test data exceeds API LIMIT for all testable streams
        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                record_count = len(expected_records[stream])
                LOGGER.info('Verifying data is sufficient for stream {}. '.format(stream) +
                      "\tRecord Count: {}\tAPI Limit: {} ".format(record_count, self.API_LIMIT.get(stream)))
                self.assertGreater(record_count, self.API_LIMIT.get(stream),
                                   msg="Pagination not ensured.\n" +
                                   "{} does not have sufficient data in expecatations.\n ".format(stream))

        # Create connection but do not use default start date
        conn_id = connections.ensure_connection(self, original_properties=False, payload_hook=self.preserve_access_token)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select=self.TESTABLE_STREAMS, select_all_fields=True
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)

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

                # Verify by pks that the replicated records match our expectations
                self.assertPKsEqual(stream, expected_records.get(stream), actual_records)
