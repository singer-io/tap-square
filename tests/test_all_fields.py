import os
import unittest

from functools import reduce
from singer import metadata
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestSquareAllFields(TestSquareBase):
    """Test that with all fields selected for a stream we replicate data as expected"""

    START_DATE = ""

    def name(self):
        return "tap_tester_square_all_fields"

    def testable_streams(self):
        return set(self.expected_streams()).difference(
            {'employees',} # STREAMS THAT CANNOT CURRENTLY BE TESTED
        )

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def test_run(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # ensure data exists for sync streams and set expectations
        self.START_DATE = self.get_properties().get('start_date')
        expected_records = {x: [] for x in self.expected_streams()} # ids by stream
        for stream in self.testable_streams():
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
            self.modify_expected_datatypes(records)

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

        # Select all available fields from all streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs, select_all_fields=True)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify all fields within each stream are selected
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
        replicated_row_count =  reduce(lambda accum,c : accum + c, first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Test by Stream
        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = list(expected_records.get(stream)[0].keys())

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

                # Verify the number of records match expectations
                self.assertEqual(len(expected_records.get(stream)),
                                 len(actual_records),
                                 msg="Number of actual records do match expectations. " +\
                                 "We probably have duplicate records.")

                # verify by values, that we replicated the expected records
                for actual_record in actual_records:
                    # Array data types need sorted for a proper comparison
                    # for key, value in actual_record.items():  # TODO this can probably be dropped?
                    #     self.sort_array_type(actual_record, key, value)
                    if not actual_record in expected_records.get(stream):
                        print("\nDATA DISCREPANCY STREAM: {}".format(stream))
                        print("Actual: {}".format(actual_record))
                        e_record = [record for record in expected_records.get(stream)
                                    if actual_record.get('eid') == record.get('eid')]
                        print("Expected: {}".format(e_record))
                        for key in schema_keys:
                            e_val = e_record[0].get(key)
                            val = actual_record.get(key)
                            if e_val != val:
                                print("\nDISCREPANCEY | KEY {}: ACTUAL: {} EXPECTED {}".format(key, val, e_val))
                    self.assertTrue(actual_record in expected_records.get(stream),
                                    msg="Actual record missing from expectations.\n" +
                                    "ACTUAL {}".format(actual_record))
                for expected_record in expected_records.get(stream):
                    if not expected_record in actual_records:
                        print("DATA DISCREPANCY")
                        print("Expected: {}".format(expected_record))
                        a_record = [record for record in actual_records
                                    if expected_record.get('eid') == record.get('eid')]
                        print("Actual: {}".format(a_record))
                    self.assertTrue(expected_record in actual_records,
                                    msg="Expected record missing from target.\n" +
                                    "EXPECTED {}".format(expected_record))


if __name__ == '__main__':
    unittest.main()
