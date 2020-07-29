import os
import unittest

import singer

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase

LOGGER = singer.get_logger()


class TestSquareIncrementalReplication(TestSquareBase):

    def name(self):
        return "tap_tester_square_incremental_replication"

    def testable_streams(self):
        return self.dynamic_data_streams().difference(
            {  # STREAMS NOT CURRENTY TESTABLE
                'employees', # Requires production environment to create records
                'modifier_lists',
                'inventories' # BUG | https://stitchdata.atlassian.net/browse/SRCE-3611
            }
        )

    def cannot_update_streams(self):
        return {
            'refunds',  # Does not have an endpoint for updating records
        }

    def streams_with_record_differences_after_create(self):
        return {
            'refunds',  # TODO: File bug with square about visible difference - provide recreatable scenario
        }

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def poll_for_updated_record(self, rec_id, start_date):
        from time import sleep
        all_payments = self.client.get_all('payments', self.START_DATE)
        temp_rec = [payment for payment in all_payments if payment['id'] == rec_id]

        for i in range(10):
            print("Polling {} iteration of payments for record: id={}".format(i, rec_id))
            sleep(2)
            all_payments = self.client.get_all('payments', self.START_DATE)
            records = [payment for payment in all_payments if payment['id'] == rec_id]
            assert len(records) == 1
            record = records[0]
            if len(temp_rec[0].keys()) < len(record.keys()):
                print("Poll Successful after {} iterations.".format(i))
                print(set(temp_rec[0].keys()).symmetric_difference(set(record.keys())))
                break

        return records

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        return sync_record_count

    def test_run(self):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is the max value sent to the target for the `date` PK field
        Verify that the 2nd sync respects the bookmark
        Verify that all data of the 2nd sync is >= the bookmark from the first sync
        Verify that the number of records in the 2nd sync is less then the first
        Verify inclusivivity of bookmarks

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        print("\n\nTESTING IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        # TODO implement PRODUCTION if necessary

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Instatiate default start date
        self.START_DATE = self.get_properties().get('start_date')

        # Ensure tested streams have existing records
        expected_records_1 = {x: [] for x in self.expected_streams()}
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream, self.START_DATE)

            if len(existing_objects) == 0:
                # TODO change from 'failure you need data' to 'create data then so we can test'
                # This can be implemented at the end of testing once all streams can be created consistently
                assert None, "NO DATA EXISTS FOR {}, SOMETHING HAS GONE TERRIBLY WRONG".format(stream)

            expected_records_1[stream] += existing_objects
            print('{}: Have sufficent amount of data to continue test'.format(stream))

        # Adjust expectations for datetime format
        for stream, expected_records in expected_records_1.items():
            print("Adjust expectations for stream: {}".format(stream))
            self.modify_expected_records(expected_records)

        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all testable streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_to_select = self.testable_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), streams_to_select,
                         msg="Expect first_sync_record_count keys {} to equal testable streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             streams_to_select,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Add data before next sync via insert and update, and set expectations
        created_records = {x: [] for x in self.expected_streams()}
        updated_records = {x: [] for x in self.expected_streams()}
        expected_records_2 = {x: [] for x in self.expected_streams()}

        # We should expect any records with rep-keys equal to the bookmark from the first sync to be returned by the second
        for order in first_sync_records['orders']['messages']:
            if order['data']['updated_at'] == first_sync_state.get('bookmarks',{}).get('orders',{}).get('updated_at'):
                expected_records_2['orders'].append(order['data'])

        for stream in self.testable_streams():
            # Create
            new_record = self.client.create(stream, start_date=self.START_DATE)
            assert len(new_record) > 0, "Failed to create a {} record".format(stream)
            if stream != 'inventories':  # This stream may have multiple records as a result of a single create
                assert len(new_record) == 1, "Created too many {} records: {}".format(stream, len(new_record))

            expected_records_2[stream] += new_record
            created_records[stream] += new_record
            if stream == 'refunds':  # a CREATE for refunds is equivalent to an UPDATE for payments
                refund = created_records[stream][0]
                payment_id = refund.get('payment_id')
                # a CREATE for refunds will result in a new payments object
                created_records['payments'].append(self.client.PAYMENTS[-1])
                expected_records_2['payments'].append(self.client.PAYMENTS[-1])

        for stream in self.testable_streams().difference(self.cannot_update_streams()):
            # Update all streams (but save payments for last)
            if stream == 'payments':
                continue
            elif stream == 'orders':
                for message in first_sync_records.get(stream).get('messages'):
                    if message.get('data')['state'] != 'COMPLETED':
                        first_rec = message.get('data')
                        break

                if not first_rec:
                    raise RuntimeError("Unable to find any any orders with state other than COMPLETED")
            else:
                first_rec = first_sync_records.get(stream).get('messages')[0].get('data')
            first_rec_id = first_rec.get('id')
            first_rec_version = first_rec.get('version')
            if stream == 'inventories':
                updated_record = self.client.update(stream, obj_id=None, version=None, obj=first_rec)
                assert len(updated_record) > 0, "Failed to update a {} record".format(stream)
            else:
                updated_record = self.client.update(stream, first_rec_id, first_rec_version)
                assert len(updated_record) > 0, "Failed to update a {} record".format(stream)
                assert len(updated_record) == 1, "Updated too many {} records".format(stream)
            expected_records_2[stream] += updated_record
            updated_records[stream] += updated_record

        # Update a Payment AFTER all other streams have been updated
        # Payments which have already completed/cancelled can't be done so again so find first APPROVED payment
        first_rec = dict()
        for message in first_sync_records.get('payments').get('messages'):
            if message.get('data')['status'] == 'APPROVED':
                first_rec = message.get('data')
                break

        if not first_rec:
            raise RuntimeError("Unable to find any any payment with status APPROVED")
        first_rec_id = first_rec.get('id')
        first_rec_version = first_rec.get('version')


        updated_record = self.client.update('payments', first_rec_id, first_rec_version)
        assert len(updated_record) > 0, "Failed to update a {} record".format('payments')
        assert len(updated_record) == 1, "Updated too many {} records".format('payments')

        updated_record = self.poll_for_updated_record(first_rec_id, self.START_DATE) #self.client.get_a_payment(first_rec_id, self.START_DATE)

        expected_records_2['payments'] += updated_record
        updated_records['payments'] += updated_record


        # adjust expectations for full table streams to include the expected records from sync 1
        for stream in self.expected_full_table_streams():
            primary_keys = self.expected_primary_keys().get(stream)
            pk = list(primary_keys)[0] if primary_keys else None
            if pk:

                updated_ids = [record.get(pk) for record in updated_records[stream]]
                for record in expected_records_1.get(stream, []):
                    if record.get(pk) in updated_ids:
                        continue  # do not add the orginal of the updated record
                    expected_records_2[stream].append(record)

            else:  # since `inventories` has no pk add all records from 1st sync
                expected_records_2[stream] += (expected_records_1.get(stream, []))

        # Adjust expectations for datetime format
        for record_desc, records in [("created", created_records), ("updated", updated_records),
                                     ("2nd sync expected records", expected_records_2)]:
            print("Adjusting epxectations for {} records".format(record_desc))
            for stream, expected_records in records.items():
                print("\tadjusting for stream: {}".format(stream))
                self.modify_expected_records(expected_records)

        # ensure validity of expected_records_2
        for stream in self.testable_streams():
            if stream in self.expected_incremental_streams():
                if stream in self.cannot_update_streams():
                    self.assertEqual(len(expected_records_2.get(stream)), 1,
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
                elif stream == 'orders': # ORDERS are returned inclusive on the datetime queried
                    self.assertEqual(len(expected_records_2.get(stream)), 3,
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
                else:  # Most streams will have 2 records from the Update and Insert
                    self.assertEqual(len(expected_records_2.get(stream)), 2,
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
            if stream in self.expected_full_table_streams():
                self.assertEqual(len(expected_records_2.get(stream)), len(expected_records_1.get(stream)) + len(created_records[stream]),
                                 msg="Expectations are invalid for full table stream {}".format(stream))

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)

        # Loop first_sync_records and compare against second_sync_records
        for stream in self.testable_streams():
            with self.subTest(stream=stream):

                second_sync_data = [record.get("data") for record
                                    in second_sync_records.get(stream, {}).get("messages", {"data": {}})]
                stream_replication_keys = self.expected_replication_keys()
                stream_primary_keys = self.expected_primary_keys()

                # TESTING INCREMENTAL STREAMS
                if stream in self.expected_incremental_streams():

                    replication_keys = stream_replication_keys.get(stream)

                    # Verify both syncs write / keep the same bookmark
                    self.assertEqual(set(first_sync_state.get('bookmarks', {}).keys()),
                                     set(second_sync_state.get('bookmarks', {}).keys()))

                    # verify that there is more than 1 record of data - setup necessary
                    self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                       msg="Data isn't set up to be able to test full sync")

                    # verify that you get less/same amount of data on the 2nd sync
                    self.assertGreater(
                        first_sync_record_count.get(stream, 0),
                        second_sync_record_count.get(stream, 0),
                        msg="first sync didn't have more records, bookmark usage not verified")


                    for replication_key in replication_keys:

                        # Verify second sync's bookmarks move past the first sync's
                        self.assertGreater(
                            second_sync_state.get('bookmarks', {stream: {}}).get(
                                stream, {replication_key: -1}).get(replication_key),
                            first_sync_state.get('bookmarks', {stream: {}}).get(
                                stream, {replication_key: -1}).get(replication_key)
                        )

                        # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                        first_sync_bookmark = first_sync_state.get('bookmarks').get(stream).get(replication_key)
                        for record in second_sync_data:
                            date_value = record[replication_key]
                            if stream == 'orders':
                                self.assertGreaterEqual(date_value,
                                                        first_sync_bookmark,
                                                        msg="A 2nd sync record has a replication-key that is less than the 1st sync bookmark.")
                            else:
                                self.assertGreater(date_value,
                                                   first_sync_bookmark,
                                                   msg="A 2nd sync record has a replication-key that is less than or equal to the 1st sync bookmark.")

                elif stream in self.expected_full_table_streams():

                    # TESTING FULL TABLE STREAMS

                    # Verify no bookmarks are present
                    first_state = first_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual(first_state, {},
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(first_sync_state) + \
                                     "\tBookmark: {}".format(first_state))
                    second_state = second_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual(second_state, {},
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(second_sync_state) + \
                                     "\tBookmark: {}".format(second_state))

                # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see only 2 records (a new record and an updated record)
                # For full table streams we should see 1 more record than the first sync
                expected_records = expected_records_2.get(stream)
                primary_keys = stream_primary_keys.get(stream)
                pk = list(primary_keys)[0] if primary_keys else None
                if stream != 'orders':  # ORDERS has too many dependencies to track explicitly
                    self.assertEqual(len(expected_records), len(second_sync_data),
                                     msg="Expected number of records do not match actual for 2nd sync.\n" +
                                     "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                    )

                if pk:  # PKs are needed for comparing records

                    # Verify that the inserted records are replicated by the 2nd sync and match our expectations
                    for created_record in created_records.get(stream):
                        sync_records = [record for record in second_sync_data
                                        if created_record.get(pk) == record.get(pk)]
                        self.assertTrue(len(sync_records),
                                        msg="An inserted record is missing from our sync: \nRECORD: {}".format(created_record))
                        self.assertEqual(len(sync_records), 1,
                                         msg="A duplicate record was found in the sync for {}\nRECORD: {}.".format(stream, sync_records))
                        sync_record = sync_records[0]
                        if stream not in self.streams_with_record_differences_after_create():
                            self.assertDictEqual(created_record, sync_record)

                    # Verify that the updated records are replicated by the 2nd sync and match our expectations
                    for updated_record in updated_records.get(stream):
                        if stream not in self.cannot_update_streams():
                            sync_records = [record for record in second_sync_data
                                            if updated_record.get(pk) == record.get(pk)]
                            self.assertTrue(len(sync_records),
                                            msg="An updated record is missing from our sync: \nRECORD: {}".format(updated_record))
                            self.assertEqual(len(sync_records), 1,
                                             msg="A duplicate record was found in the sync for {}\nRECORDS: {}.".format(stream, sync_records))
                            sync_record = sync_records[0]

                            # TODO | TEST ISSUE | Address delayed fields in updated payments
                            if stream == 'payments' and sync_record.get('processing_fee') and updated_record.get('processing_fee') is None:
                                updated_record['processing_fee'] = sync_record.get('processing_fee')

                            self.assertDictEqual(updated_record, sync_record)

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


if __name__ == '__main__':
    unittest.main()
