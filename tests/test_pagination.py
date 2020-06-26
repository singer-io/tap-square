import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import os
import logging
from datetime import timedelta, date
from datetime import datetime as dt
from functools import reduce

from base import TestSquareBase

class TestSquarePagination(TestSquareBase):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""
    API_LIMIT = 1000

    def name(self):
        return "tap_tester_square_pagination_test"

    def testable_streams(self):
        return self.expected_incremental_streams()

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Ensure tested streams have a record count which exceeds the API LIMIT
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream, self.START_DATE)
            if len(existing_objects) <= self.API_LIMIT:
                num_to_post = 1001-len(existing_objects)
                print('{}: Will create {} records'.format(stream, num_to_post))
                self.client.create_batch_post(stream, num_to_post)
                print('{}: Created {} records'.format(stream, num_to_post))
            else:
                print('{}: Have sufficent amount of data to continue test'.format(stream))

        # Create connection with default start date
        conn_id = connections.ensure_connection(self)

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
        exclude_streams = list(self.expected_streams().difference(self.testable_streams()))
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
                                                                         self.testable_streams(),
                                                                         self.expected_primary_keys())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        schemas = {catalog['tap_stream_id']: menagerie.get_annotated_schema(conn_id, catalog['stream_id']) for catalog in found_catalogs}
        all_fields = {stream: set(schema['annotated-schema']['properties'].keys()) for stream, schema in schemas.items()}

        for stream in self.testable_streams():
            with self.subTest(stream=stream):

                # Verify we are paginating for testable synced streams
                self.assertGreater(record_count_by_stream.get(stream, -1), self.API_LIMIT,
                                   msg="We didn't guarantee pagination. The number of records should exceed the api limit.")

                data = synced_records.get(stream, [])
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]

                for actual_keys in record_messages_keys:

                    # Verify that the automatic fields are sent to the target for paginated streams
                    self.assertEqual(all_fields.get(stream) - actual_keys,
                                     set(), msg="A paginated synced stream has a record that is missing automatic fields.")

                    # Verify we have more fields sent to the target than just automatic fields (this is set above)
                    self.assertGreater(actual_keys, self.expected_automatic_fields().get(stream),
                                      msg="A paginated synced stream has a record that is missing non-automatic fields.")
