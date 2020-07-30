import os

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from datetime import timedelta, date
#from datetime import datetime as dt
from singer import utils
from base import TestSquareBase


class TestSquarePagination(TestSquareBase):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""

    DEFAULT_BATCH_LIMIT = 1000
    API_LIMIT = {
        'items': DEFAULT_BATCH_LIMIT,
        'inventories': DEFAULT_BATCH_LIMIT,
        'categories': DEFAULT_BATCH_LIMIT,
        'discounts': DEFAULT_BATCH_LIMIT,
        'taxes': DEFAULT_BATCH_LIMIT,
        'employees': 50,
        'locations': None, # TODO
        'refunds': 100,
        'payments': 100,
        'modifier_lists': None, # TODO
        'orders': 500,
        'shifts': 200,
    }

    def name(self):
        return "tap_tester_square_pagination_test"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS NOT CURRENTY TESTABLE
                'employees', # Requires production environment to create records
                'modifier_lists',
                'roles' #only works with prod app
            }
        )

    def testable_streams_static(self):
        return self.static_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'locations',  # Only 300 locations can be created, and 300 are returned in a single request
                'bank_accounts', # Cannot create a record, also PROD ONLY
                'roles'
            }
        )

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))

        print("\n\nTESTING WITH DYNAMIC DATA")
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams()
        self.pagination_test()

        print("\n\nTESTING WITH STATIC DATA")
        # TODO Uncomment once TASK addressed https://stitchdata.atlassian.net/browse/SRCE-3575
        # self.START_DATE = self.STATIC_START_DATE
        # self.TESTABLE_STREAMS = self.testable_streams_static()
        # self.pagination_test()

        # TODO implement PRODUCTION

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

        if self.TESTABLE_STREAMS == set(): # REMOVE once we are testing a static stream
            print("WE ARE SKIPPING THIS TEST\n\n")

        # Ensure tested streams have a record count which exceeds the API LIMIT
        expected_records = {x: [] for x in self.expected_streams()}
        for stream in self.TESTABLE_STREAMS:
            existing_objects = self.client.get_all(stream, self.START_DATE)
            if len(existing_objects) == 0:
               print("NO DATA EXISTS FOR STREAM {}".format(stream))

            expected_records[stream] += existing_objects

            if len(existing_objects) <= self.API_LIMIT.get(stream):
                num_records = self.API_LIMIT.get(stream) + 1 - len(existing_objects)
                print('{}: Will create {} records'.format(stream, num_records))
                new_objects = []
                if stream == 'orders':
                    location_id = [location['id'] for location in self.client.get_all('locations')][0]
                    for i in range(num_records):
                        new_objects.append(self.client.create_order(location_id))
                elif stream == 'shifts':
                    # Find the max end_at to know when the last shift ends, so we can start a shift there
                    max_end_at = max([obj['end_at'] for obj in existing_objects])
                    end_at_datetime = utils.strptime_to_utc(max_end_at)
                    for i in range(num_records):
                        # Tested in the API Explorer that +00:00 works
                        # How does this work? It feels like it should be new_objects += blah
                        new_objects.append(
                            # Create a shift that is self.client.SHIFT_MINUTES long
                            self.client.create('shifts', start_date=utils.strftime(end_at_datetime))
                        )
                        # Bump our known max `end_at` by self.client.SHIFT_MINUTES
                        end_at_datetime = end_at_datetime + timedelta(minutes=self.client.SHIFT_MINUTES)

                elif stream in {'employees', 'refunds', 'payments'}: # non catalog objectsx
                    for n in range(num_records):
                        print('{}: Created {} records'.format(stream, n))
                        new_object = self.client.create(stream, start_date=self.START_DATE)
                        assert new_object[0], "Failed to create a {} record.\nRECORD: {}".format(stream, new_object[0])
                        new_objects += new_object
                elif stream == 'inventories':
                    new_objects = self.client.create_batch_inventory_adjustment(num_records).body.get('counts', [])
                elif stream in {'items', 'categories', 'discounts', 'taxes'}:  # catalog objects
                    new_objects = self.client.create_batch_post(stream, num_records).body.get('objects', [])

                else:
                    raise RuntimeError("The stream {} is missing from the setup.".format(stream))
                expected_records[stream] += new_objects

                print('{}: Created {} records'.format(stream, num_records))
            else:
                print('{}: Have sufficent amount of data to continue test'.format(stream))

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
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        #select all catalogs
        exclude_streams = list(self.expected_streams().difference(self.TESTABLE_STREAMS))
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs,
                                           exclude_streams=exclude_streams)

        #clear state
        menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                         self.TESTABLE_STREAMS,
                                                                         self.expected_primary_keys())
        replicated_row_count =  sum(record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        schemas = {catalog['tap_stream_id']: menagerie.get_annotated_schema(conn_id, catalog['stream_id']) for catalog in found_catalogs}
        all_fields = {stream: set(schema['annotated-schema']['properties'].keys()) for stream, schema in schemas.items()}

        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):

                # Verify we are paginating for testable synced streams
                self.assertGreater(record_count_by_stream.get(stream, -1), self.API_LIMIT.get(stream),
                                   msg="We didn't guarantee pagination. The number of records should exceed the api limit.")

                data = synced_records.get(stream, [])
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                auto_fields = self.expected_automatic_fields().get(stream)

                for actual_keys in record_messages_keys:

                    # Verify that the automatic fields are sent to the target for paginated streams
                    self.assertTrue(auto_fields.issubset(actual_keys),
                                    msg="A paginated synced stream has a record that is missing automatic fields.")

                    # Verify we have more fields sent to the target than just automatic fields (this is set above)
                    self.assertEqual(auto_fields.difference(actual_keys),
                                     set(), msg="A paginated synced stream has a record that is missing expected fields.")

                # TODO ADD CHECK ON IDS
                # Verify by pks that the data replicated matches what we expect


        print("\n\n\t TODO STREAMS NOT UNDER TEST: {}".format(
            self.expected_streams().difference(self.TESTABLE_STREAMS))
        )

if __name__ == '__main__':
    unittest.main()
