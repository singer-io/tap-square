import os

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase
from test_client import TestClient


class TestAutomaticFields(TestSquareBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    def name(self):
        return "tap_tester_square_automatic_fields"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS NOT CURRENTY TESTABLE
                'employees',
                'modifier_lists', # TODO must be added once creates and updates are available
            }
        )

    def testable_streams_static(self):
        return self.static_data_streams().difference(
            {  # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'bank_accounts'
            }
        )

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        print("\n\nTESTING WITH DYNAMIC DATA")
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams().difference(self.production_streams())
        self.auto_fields_test()

        print("\n\nTESTING WITH STATIC DATA")
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.auto_fields_test()

        # TODO PRODUCTION is not fully configured
        # self.set_environment(self.PRODUCTION)
        # print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        # print("\n\nTESTING WITH STATIC DATA")
        # self.START_DATE = self.get_properties().get('start_date')
        # self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.sandbox_streams())
        # self.auto_fields_test()

    def auto_fields_test(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.TESTABLE_STREAMS))

        # ensure data exists for sync streams and set expectations
        expected_records = {x: [] for x in self.expected_streams()}
        for stream in self.TESTABLE_STREAMS:
            existing_objects = self.client.get_all(stream, self.START_DATE)
            if not existing_objects:
                print("Test data is not properly set for {}.".format(stream))

                new_record = self.client.create(stream, start_date=self.START_DATE)
                assert len(new_record) > 0, "Failed to create a {} record".format(stream)
                assert len(new_record) == 1, "Created too many {} records: {}".format(stream, len(new_record))

                expected_records[stream] += new_record

            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records[stream].append(
                    {field: obj.get(field)
                     for field in self.expected_automatic_fields().get(stream)}
                )

        # Adjust expectations for datetime format
        for stream, records in expected_records.items():
            print("Adjust expectations for stream: {}".format(stream))
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


        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify that pks, rep keys, foreign keys have inclusion of automatic (metadata and annotated schema).
            for k in self.expected_automatic_fields().get(cat['stream_name']):
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)

                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

        # Select testable streams. Deselect all available fields from all testable streams, keep automatic fields
        exclude_streams = self.expected_streams().difference(self.TESTABLE_STREAMS)
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=False, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in catalogs:
            expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in self.TESTABLE_STREAMS:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")
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
                expected_keys = self.expected_automatic_fields().get(stream)
                schema_keys = set(self.expected_schema_keys(stream))
                primary_keys = self.expected_primary_keys().get(stream)

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

                # Test by keys and values, that we replicated the expected records and nothing else

                # Verify that actual records were in our expectations
                for actual_record in actual_records:
                    if len(primary_keys) > 1:
                        pk_1, pk_2 = primary_keys
                        stream_expected_records = [record for record in expected_records.get(stream)
                                                   if actual_record.get(pk_1) == record.get(pk_1) and
                                                   actual_record.get(pk_2) == record.get(pk_2)]
                    else:
                        pk = list(primary_keys)[0]
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
                    if len(primary_keys) > 1:
                        pk_1, pk_2 = primary_keys
                        stream_actual_records = [record for record in actual_records
                                                 if expected_record.get(pk_1) == record.get(pk_1) and
                                                 expected_record.get(pk_2) == record.get(pk_2)]
                    else:
                        pk = list(primary_keys)[0]
                        stream_actual_records = [record for record in actual_records
                                                 if expected_record.get(pk) == record.get(pk)]
                    self.assertTrue(len(stream_actual_records),
                                    msg="An expected record is missing from the sync: \nRECORD: {}".format(expected_record))
                    self.assertEqual(len(stream_actual_records), 1,
                                     msg="A duplicate record was found in the sync for {}.".format(stream))
                    stream_actual_record = stream_actual_records[0]
                    self.assertDictEqual(expected_record, stream_actual_record)
