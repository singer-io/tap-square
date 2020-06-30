from collections import defaultdict
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestAutomaticFields(TestSquareBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    def name(self):
        return "tap_tester_square_automatic_fields"

    def test_run(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # ensure data exists for sync streams and set expectations
        expected_records = defaultdict(list)
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream, self.START_DATE)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))

            for obj in existing_objects:
                expected_records[stream].append(
                    {field: obj.get(field)
                     for field in self.expected_automatic_fields().get(stream)}
                )

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


        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify that pks, rep keys, foreign keys have inclusion of automatic (metadata and annotated schema).
            for k in self.expected_automatic_fields().get(cat['stream_name']):
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)

                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

        # Deselect all available fields from all streams, keep automatic fields
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs, select_all_fields=False)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in found_catalogs:
            expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            self.assertTrue(selected, msg="Stream not selected by default")
            # Verify only automatic fields are selected
            selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
            self.assertEqual(expected_automatic_fields, selected_fields)

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
                expected_keys = self.expected_automatic_fields().get(stream)


                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(expected_keys), set(),
                        msg="Expected automatic fields and nothing else.")

                actual_records = [row['data'] for row in data['messages']]

                # Verify the number of records match expectations
                self.assertEqual(len(expected_records.get(stream)),
                                 len(actual_records),
                                 msg="Number of actual records do match expectations. " +\
                                 "We probably have duplicate records.")

                # verify by values, that we replicated the expected records
                for actual_record in actual_records:
                    if not actual_record in expected_records.get(stream):
                        print("\n==== DATA DISCREPANCY ====\n")
                        print("Expected: {}\n".format(actual_record))
                        e_record = [record for record in expected_records.get(stream)
                                    if actual_record.get('eid') == record.get('eid')]
                        print("Actual: {}\n".format(e_record))
                        for key in schema_keys:
                            e_val = e_record[0].get(key)
                            val = actual_record.get(key)
                            if e_val != val:
                                print("\nDISCREPANCEY | KEY {}: ACTUAL: {} EXPECTED {}".format(key, val, e_val))
                    self.assertTrue(actual_record in expected_records.get(stream),
                                    msg="Actual record missing from expectations")
                for expected_record in expected_records.get(stream):
                    if not expected_record in actual_records:
                        print("\n==== DATA DISCREPANCY ====\n")
                        print("Expected: {}\n".format(expected_record))
                        a_record = [record for record in actual_records
                                    if expected_record.get('eid') == record.get('eid')]
                        print("Actual: {}\n".format(a_record))
                    self.assertTrue(expected_record in actual_records,
                                    msg="Expected record missing from target.")
