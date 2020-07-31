import os
import unittest

from singer import metadata
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestSquareAllFields(TestSquareBase):
    """Test that with all fields selected for a stream we replicate data as expected"""
    TESTABLE_STREAMS = set()

    def name(self):
        return "tap_tester_square_all_fields"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'employees',
                'items',  # BUG | https://stitchdata.atlassian.net/browse/SRCE-3606
                'modifier_lists',
                'roles'# only works with prod
                'settlements',
                'cash_drawer_shifts',
            }
        )

    def testable_streams_static(self):
        return self.static_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'bank_accounts', # Cannot create a record, also PROD ONLY
            }
        )

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        print("\n\nTESTING WITH DYNAMIC DATA")
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams()
        self.all_fields_test()

        print("\n\nTESTING WITH STATIC DATA")
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static()
        self.all_fields_test()

        # TODO implement PRODUCTION

    def all_fields_test(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        if self.TESTABLE_STREAMS == set(): # REMOVE once BUG addressed
            print("WE ARE SKIPPING THIS TEST\n\n")

        # ensure data exists for sync streams and set expectations
        expected_records = {x: [] for x in self.expected_streams()} # ids by stream
        for stream in self.TESTABLE_STREAMS:
            existing_objects = self.client.get_all(stream, self.START_DATE)
            if existing_objects:
                print("Data exists for stream: {}".format(stream))
                for obj in existing_objects:
                    expected_records[stream].append(obj)
            else:
               print("Data does not exist for stream: {}".format(stream))
               assert None, "more test functinality needed"

        # modify data set to conform to expectations (json standards)
        for stream, records in expected_records.items():
            print("Ensuring expected data for {} has values formatted correctly.".format(stream))
            self.modify_expected_records(records)

        # Instantiate connection with default start/end dates
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

        # Select all available fields from all testable streams
        exclude_streams = self.expected_streams().difference(self.TESTABLE_STREAMS)
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=True, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in self.TESTABLE_STREAMS:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify all fields within each selected stream are selected
            for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                field_selected = field_props.get('selected')
                print("\tValidating selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))
                self.assertTrue(field_selected, msg="Field not selected.")

        #clear state
        menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        first_record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                         self.expected_streams(),
                                                                         self.expected_primary_keys())
        replicated_row_count =  sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Test by Stream
        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = list(expected_records.get(stream)[0].keys())
                primary_keys = self.expected_primary_keys().get(stream)
                pk = list(primary_keys)[0] if primary_keys else None

                # Verify schema covers all fields
                schema_keys = set(self.expected_schema_keys(stream))
                self.assertEqual(
                    set(), set(expected_keys).difference(schema_keys),
                    msg="\nFields missing from schema: {}\n".format(set(expected_keys).difference(schema_keys))
                )

                # not a test, just logging the fields that are included in the schema but not in the expectations
                if schema_keys.difference(set(expected_keys)):
                    print("WARNING Fields missing from expectations: {}".format(schema_keys.difference(set(expected_keys))))

                # Verify that all fields sent to the target fall into the expected schema
                for actual_keys in record_messages_keys:
                    self.assertTrue(
                        actual_keys.issubset(schema_keys),
                        msg="Expected all fields to be present, as defined by schemas/{}.json".format(stream) +
                        "EXPECTED (SCHEMA): {}\nACTUAL (REPLICATED KEYS): {}".format(schema_keys, actual_keys))

                actual_records = [row['data'] for row in data['messages']]

                if pk:

                    stream_expected_record_ids = {record[pk] for record in expected_records.get(stream)}
                    stream_actual_record_ids = {record[pk] for record in actual_records}
                    self.assertEqual(stream_expected_record_ids,
                                     stream_actual_record_ids)

                    # Test by keys and values, that we replicated the expected records and nothing else

                    # Verify that actual records were in our expectations
                    for actual_record in actual_records:
                        stream_expected_records = [record for record in expected_records.get(stream)
                                                   if actual_record.get(pk) == record.get(pk)]
                        self.assertTrue(len(stream_expected_records),
                                        msg="An actual record is missing from our expectations: \nRECORD: {}".format(actual_record))
                        self.assertEqual(len(stream_expected_records), 1,
                                         msg="A duplicate record was found in our expectations for {}.".format(stream))
                        stream_expected_record = stream_expected_records[0]
                        self.assertDictEqual(actual_record, stream_expected_record)


                    # Verify that our expected records were replicated by the tap
                    for expected_record in expected_records.get(stream):
                        stream_actual_records = [record for record in actual_records
                                                 if expected_record.get(pk) == record.get(pk)]
                        self.assertTrue(len(stream_actual_records),
                                        msg="An expected record is missing from the sync: \nRECORD: {}".format(expected_record))
                        self.assertEqual(len(stream_actual_records), 1,
                                         msg="A duplicate record was found in the sync for {}.".format(stream))
                        stream_actual_record = stream_actual_records[0]
                        self.assertDictEqual(expected_record, stream_actual_record)

                else:  # 'inventories' does not have a pk so our assertions aren't as clean

                    # Verify that actual records were in our expectations
                    for actual_record in actual_records:
                        if actual_record not in expected_records.get(stream):
                            print("DATA DISCREPANCY:\n\nACTUAL RECORD:\n{}\n".format(actual_record))
                            for record in expected_records.get(stream):
                                if record.get('catalog_object_id') == actual_record.get('catalog_object_id') and \
                                   record.get('location_id') == actual_record.get('location_id'):
                                    print("EXPECTED_RECORDS:")
                                    print(str(record))
                        self.assertIn(actual_record, expected_records.get(stream))

                    # Verify that our expected records were replicated by the tap
                    for expected_record in expected_records.get(stream):
                        if expected_record not in actual_records:
                            print("DATA DISCREPANCY:\n\nEXPECTED RECORD:\n{}\n".format(expected_record))
                            for record in actual_records:
                                if record.get('catalog_object_id') == expected_record.get('catalog_object_id') and \
                                   record.get('location_id') == expected_record.get('location_id'):
                                    print("ACTUAL_RECORDS:")
                                    print(str(record))
                        self.assertIn(expected_record, actual_records)

                    self.assertEqual(len(expected_records.get(stream)), len(actual_records),
                                     msg="Unexpected number of records synced.")


if __name__ == '__main__':
    unittest.main()
