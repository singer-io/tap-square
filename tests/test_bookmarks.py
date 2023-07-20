import os
from time import perf_counter

import singer
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBaseParent

LOGGER = singer.get_logger()


class TestSquareIncrementalReplication(TestSquareBaseParent.TestSquareBase):

    @staticmethod
    def name():
        return "tap_tester_square_incremental_replication"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    @staticmethod
    def testable_streams_static():
        """ No static streams marked for incremental. """
        return set()

    @staticmethod
    def cannot_update_streams():
        return {
            'refunds',  # Does not have an endpoint for updating records
            'modifier_lists',  # Has endpoint but just adds/removes mod_list from an item.
        }

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream

        TEST_ISSUE_1 | https://stitchdata.atlassian.net/browse/SRCE-4690
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
        """Instantiate start date according to the desired data set and run the test"""

        self.START_DATE = self.get_properties().get('start_date')

        print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.bookmarks_test(self.testable_streams_dynamic().intersection(self.sandbox_streams()) - {'customers', 'inventories', 'orders', 'items', 'team_members', 'discounts', 'categories', 'taxes', 'modifier_lists', 'refunds', 'payments'})

        # self.set_environment(self.PRODUCTION)
        # 
        # production_testable_streams = self.testable_streams_dynamic().intersection(self.production_streams())
        # 
        # if production_testable_streams:
        #     print("\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}".format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        #     self.bookmarks_test(production_testable_streams)

    def bookmarks_test(self, testable_streams):
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
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Ensure tested streams have existing records
        expected_records_first_sync = self.create_test_data(testable_streams, self.START_DATE, force_create_records=True)

        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all testable streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_to_select = testable_streams
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(streams_to_select, set(first_sync_record_count.keys()),
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
        expected_records_second_sync = {x: [] for x in self.expected_streams()}

        # We should expect any records with rep-keys equal to the bookmark from the first sync to be returned by the second
        if 'orders' in testable_streams:
            for order in first_sync_records['orders']['messages']:
                if order['data']['updated_at'] == first_sync_state.get('bookmarks', {}).get('orders', {}).get('updated_at'):
                    expected_records_second_sync['orders'].append(order['data'])

        streams_to_create_records = list(testable_streams)
        if 'payments' in testable_streams:
            streams_to_create_records.remove('payments')
            streams_to_create_records.append('payments')

        for stream in streams_to_create_records:

            new_records = []

            if stream == 'refunds':  # a CREATE for refunds is equivalent to an UPDATE for payments
                # a CREATE for refunds will result in a new payments object
                (new_refund, payment) = self.client.create_refund(start_date=self.START_DATE)
                new_records = new_refund

                created_records['payments'].append(payment)
                expected_records_second_sync['payments'].append(payment)
            else:
                # TEST_ISSUE_1 | get the time that the customer record was created
                if stream == 'customers':
                    customers_create_time = perf_counter()

                # Create
                new_records = self.client.create(stream, start_date=self.START_DATE)

            assert new_records, "Failed to create a {} record".format(stream)
            assert len(new_records) == 1, "Created too many {} records: {}".format(stream, len(new_records))
            expected_records_second_sync[stream] += new_records
            created_records[stream] += new_records

        for stream in testable_streams.difference(self.cannot_update_streams()):
            first_rec = None
            # Update all streams (but save payments for last)
            if stream == 'payments':
                continue

            if stream == 'orders':  # Use the first available order that is still 'OPEN'
                for message in first_sync_records.get(stream).get('messages'):
                    if message.get('data')['state'] not in ['COMPLETED', 'CANCELED']:
                        first_rec = message.get('data')
                        break

                if not first_rec:
                    raise RuntimeError("Unable to find any any orders with state other than COMPLETED")
            elif stream == 'roles':  # Use the first available role that has limited permissions (where is_owner = False)
                for message in first_sync_records.get(stream).get('messages'):
                    data = message.get('data')
                    if not data['is_owner'] and 'role' in data['name']:
                        first_rec = message.get('data')
                        break

                if not first_rec:
                    raise RuntimeError("Unable to find any any orders with state other than COMPLETED")
            else: # By default we want the last created record
                last_message = first_sync_records.get(stream).get('messages')[-1]
                if last_message.get('data') and not last_message.get('data').get('is_deleted'):
                    first_rec = last_message.get('data')
                else: # If last record happens to be deleted grab first available that wasn't
                    LOGGER.warning("The last created record for %s was deleted.", stream)
                    for message in first_sync_records.get(stream).get('messages'):
                        data = message.get('data')
                        if not data.get('is_deleted'):
                            first_rec = message.get('data')
                            break

                if not first_rec:
                    raise RuntimeError("Cannot find any {} records that were not deleted .".format(stream))

            if stream == 'inventories': # This is an append only stream, we will make multiple 'updates'
                first_rec_catalog_obj_id = first_rec.get('catalog_object_id')
                first_rec_location_id = first_rec.get('location_id')
                # IN_STOCK -> SOLD [quantity -1]
                updated_record = self.client.create_specific_inventory_adjustment(
                    first_rec_catalog_obj_id, first_rec_location_id,
                    from_state='IN_STOCK', to_state='SOLD', quantity='1.0')
                assert len(updated_record) == 1, "Failed to update the {} records as intended".format(stream)
                # UNLINKED_RETURN -> IN_STOCK [quantity +1]
                updated_record = self.client.create_specific_inventory_adjustment(
                    first_rec_catalog_obj_id, first_rec_location_id,
                    from_state='UNLINKED_RETURN', to_state='IN_STOCK', quantity='2.0')
                assert len(updated_record) == 1, "Failed to update the {} records as intended".format(stream)
                # NONE -> IN_STOCK [quantity +2]
                updated_record = self.client.create_specific_inventory_adjustment(
                    first_rec_catalog_obj_id, first_rec_location_id,
                    from_state='NONE', to_state='IN_STOCK', quantity='1.0')
                assert len(updated_record) == 1, "Failed to update the {} records as intended".format(stream)
                # IN_STOCK -> WASTE [quantity +1]
                updated_record = self.client.create_specific_inventory_adjustment(
                    first_rec_catalog_obj_id, first_rec_location_id,
                    from_state='IN_STOCK', to_state='WASTE', quantity='1.0')  # creates 2 records
                assert len(updated_record) == 2, "Failed to update the {} records as intended".format(stream)
            else:
                first_rec_id = first_rec.get('id')
                first_rec_version = first_rec.get('version')

                if stream == 'customers':  # TEST_ISSUE_1 get the time that the customer record was updated
                    customers_update_time = perf_counter()

                updated_record = self.client.update(stream, obj_id=first_rec_id, version=first_rec_version,
                                                    obj=first_rec, start_date=self.START_DATE)
                assert updated_record, "Failed to update a {} record".format(stream)

                assert len(updated_record) == 1, "Updated too many {} records".format(stream)

            expected_records_second_sync[stream] += updated_record

            updated_records[stream] += updated_record

        if 'payments' in testable_streams:
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
            assert updated_record, "Failed to update a {} record".format('payments')
            assert len(updated_record) == 1, "Updated too many {} records".format('payments')

            expected_records_second_sync['payments'] += updated_record[0]
            updated_records['payments'] += updated_record[0]

        # adjust expectations for full table streams to include the expected records from sync 1
        for stream in self.expected_full_table_streams():
            if stream == 'inventories':
                primary_keys = self.makeshift_primary_keys().get(stream)
            else:
                primary_keys = list(self.expected_primary_keys().get(stream))
            updated_pk_values = {tuple([record.get(pk) for pk in primary_keys]) for record in updated_records[stream]}
            for record in expected_records_first_sync.get(stream, []):
                record_pk_values = tuple([record.get(pk) for pk in primary_keys])
                if record_pk_values in updated_pk_values:
                    continue  # do not add the orginal of the updated record
                expected_records_second_sync[stream].append(record)

        # Adjust expectations for datetime format
        for record_desc, records in [("created", created_records), ("updated", updated_records),
                                     ("2nd sync expected records", expected_records_second_sync)]:
            print("Adjusting epxectations for {} records".format(record_desc))
            for stream, expected_records in records.items():
                print("\tadjusting for stream: {}".format(stream))
                self.modify_expected_records(expected_records)

        # ensure validity of expected_records_second_sync
        for stream in testable_streams:
            if stream in self.expected_incremental_streams():
                if stream in self.cannot_update_streams():
                    self.assertEqual(1, len(expected_records_second_sync.get(stream)),
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
                elif stream == 'orders': # ORDERS are returned inclusive on the datetime queried
                    self.assertEqual(3, len(expected_records_second_sync.get(stream)),
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
                else:  # Most streams will have 2 records from the Update and Insert
                    self.assertEqual(2, len(expected_records_second_sync.get(stream)),
                                     msg="Expectations are invalid for incremental stream {}".format(stream))
            if stream in self.expected_full_table_streams():
                if stream == 'inventories':
                    # Typically changes to inventories object will replace an IN_STOCK record with two records
                    #    1 IN_STOCK  ->  1 IN_STOCK, 1 WASTE
                    # if a given combination of {'catalog_object_id', 'location_id', 'state'} already has a
                    # WASTE record then both records will be replaced
                    #    1 IN_STOCK, 1 WASTE  ->  1 IN_STOCK, 1 WASTE
                    self.assertLessEqual(
                        len(expected_records_second_sync.get(stream)),
                        len(expected_records_first_sync.get(stream)) + len(created_records[stream]) + 1,
                        msg="Expectations are invalid for full table stream {}".format(stream))
                    self.assertGreaterEqual(
                        len(expected_records_second_sync.get(stream)),
                        len(expected_records_first_sync.get(stream)) + len(created_records[stream]),
                        msg="Expectations are invalid for full table stream {}".format(stream))
                    continue
                self.assertEqual(len(expected_records_second_sync.get(stream)), len(expected_records_first_sync.get(stream)) + len(created_records[stream]),
                                 msg="Expectations are invalid for full table stream {}".format(stream))

        # Run a second sync job using orchestrator
        second_sync_time_start = perf_counter()  # TEST_ISSUE_1 get the time that the 2nd sync starts
        second_sync_record_count = self.run_sync(conn_id)
        second_sync_time_end = perf_counter()  # TEST_ISSUE_1 get the time that the 2nd sync ends

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)


        # BUG_1 | https://stitchdata.atlassian.net/browse/SRCE-4975
        PARENT_FIELD_MISSING_SUBFIELDS = {'payments': {'card_details'}}

        # BUG_2 | https://stitchdata.atlassian.net/browse/SRCE-5143
        MISSING_FROM_SCHEMA = {'payments': {'capabilities', 'version_token', 'approved_money'}}


        # Loop first_sync_records and compare against second_sync_records
        for stream in testable_streams:
            with self.subTest(stream=stream):

                second_sync_data = [record.get("data") for record
                                    in second_sync_records.get(stream, {}).get("messages", [])]
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
                            self.assertGreaterEqual(date_value,
                                                    first_sync_bookmark,
                                                    msg="A 2nd sync record has a replication-key that is less than or equal to the 1st sync bookmark.")

                elif stream in self.expected_full_table_streams():

                    # TESTING FULL TABLE STREAMS

                    # Verify no bookmarks are present
                    first_state = first_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual({}, first_state,
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(first_sync_state) + \
                                     "\tBookmark: {}".format(first_state))
                    second_state = second_sync_state.get('bookmarks', {}).get(stream)
                    self.assertEqual({}, second_state,
                                     msg="Unexpected state for {}\n".format(stream) + \
                                     "\tState: {}\n".format(second_sync_state) + \
                                     "\tBookmark: {}".format(second_state))

                if stream == 'customers' and len(second_sync_data) == 0: # BUG https://stitchdata.atlassian.net/browse/SRCE-4639
                    # NOTE: Square sometimes lags on the customers stream, so we'll give them one more shot
                    #       before we say this stream fails in catching the create and update. This was tested
                    #       manually while syncing all streams and while sycning only the customers stream
                    #       and we were unable to produce a scenario in which a subsequent sync failed to pick
                    #       up the create and update after failing to catch them in the 2nd sync.

                    # TEST_ISSUE_1 | Log the time diffs for record created, updated, second sync ran
                    LOGGER.warning(
                        'Second sync missed %s records that were just created and updated.\n' +
                        'Time between record create and: \n\tsync start = %s\tsync end: %s\n' +
                        'Time between record update and: \n\tsync start = %s\tsync end: %s',
                        stream,
                        second_sync_time_start - customers_create_time, second_sync_time_end - customers_create_time,
                        second_sync_time_start - customers_update_time, second_sync_time_end - customers_update_time,
                    )

                    # TODO TIMING | get the time the third sync ran
                    # Run another sync since square can't keep up
                    third_sync_time_start = perf_counter()  # TEST_ISSUE_1 get the time that the 3rd sync starts
                    _ = self.run_sync(conn_id)
                    third_sync_time_end = perf_counter()  # TEST_ISSUE_1 get the time that the 3rd sync ends

                    # Get the set of records from a thrid sync and apply
                    third_sync_records = runner.get_records_from_target_output()
                    second_sync_data = [record.get("data") for record
                                        in third_sync_records.get(stream, {}).get("messages", [])]
                else:  # TEST_ISSUE_1
                    third_sync_time_start = perf_counter()
                    third_sync_time_end = perf_counter()

                # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see at least 2 records (a new record and an updated record)
                # but we may see more as the bookmmark is inclusive.
                # For full table streams we should see 1 more record than the first sync
                expected_records = expected_records_second_sync.get(stream)
                if stream == 'inventories':
                    primary_keys = self.makeshift_primary_keys().get(stream)
                else:
                    primary_keys = stream_primary_keys.get(stream)

                updated_pk_values = {tuple([record.get(pk) for pk in primary_keys]) for record in updated_records[stream]}

                if stream == 'customers' and len(second_sync_data) != len(expected_records): # TEST_ISSUE_1
                    # TEST_ISSUE_1 | Log the time diffs for record created, updated, third sync ran
                    LOGGER.warning(
                        'Third sync missed %s records that were just created and updated.\n' +
                        'Time between record create and: \n\tsync start = %s\tsync end: %s\n' +
                        'Time between record update and: \n\tsync start = %s\tsync end: %s',
                        stream,
                        third_sync_time_start - customers_create_time, third_sync_time_end - customers_create_time,
                        third_sync_time_start - customers_update_time, third_sync_time_end - customers_update_time,
                    )

                self.assertLessEqual(
                    len(expected_records), len(second_sync_data),
                    msg="Expected number of records are not less than or equal to actual for 2nd sync.\n" +
                    "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                )
                if (len(second_sync_data) - len(expected_records)) > 0:
                    LOGGER.warning('Second sync replicated %s records more than our create and update for %s',
                                   len(second_sync_data), stream)

                if not primary_keys:
                    raise NotImplementedError("PKs are needed for comparing records")

                # Verify that the inserted records are replicated by the 2nd sync and match our expectations
                for created_record in created_records.get(stream):
                    record_pk_values = tuple([created_record.get(pk) for pk in primary_keys])
                    sync_records = [sync_record for sync_record in second_sync_data
                                    if tuple([sync_record.get(pk) for pk in primary_keys]) == record_pk_values]
                    self.assertTrue(len(sync_records),
                                    msg="An inserted record is missing from our sync: \nRECORD: {}".format(created_record))
                    self.assertEqual(1, len(sync_records),
                                     msg="A duplicate record was found in the sync for {}\nRECORD: {}.".format(stream, sync_records))
                    sync_record = sync_records[0]
                    # Test Workaround Start ##############################
                    if stream == 'payments':

                        off_keys = MISSING_FROM_SCHEMA[stream] # BUG_2
                        self.assertParentKeysEqualWithOffKeys(
                            created_record, sync_record, off_keys
                        )
                        off_keys = PARENT_FIELD_MISSING_SUBFIELDS[stream] | MISSING_FROM_SCHEMA[stream] # BUG_1 | # BUG_2
                        self.assertDictEqualWithOffKeys(
                            created_record, sync_record, off_keys
                        )  # Test Workaround End ##############################

                    else:
                        self.assertRecordsEqual(stream, created_record, sync_record)

                # Verify that the updated records are replicated by the 2nd sync and match our expectations
                for updated_record in updated_records.get(stream):
                    if stream not in self.cannot_update_streams():
                        record_pk_values = tuple([updated_record.get(pk) for pk in primary_keys])
                        sync_records = [sync_record for sync_record in second_sync_data
                                        if tuple([sync_record.get(pk) for pk in primary_keys]) == record_pk_values]
                        if stream != 'modifier_lists':
                            self.assertTrue(len(sync_records),
                                            msg="An updated record is missing from our sync: \nRECORD: {}".format(updated_record))
                            self.assertEqual(1, len(sync_records),
                                             msg="A duplicate record was found in the sync for {}\nRECORDS: {}.".format(stream, sync_records))

                        sync_record = sync_records[0]

                        # Test Workaround Start ##############################
                        if stream == 'payments':

                            off_keys = MISSING_FROM_SCHEMA[stream] # BUG_2
                            self.assertParentKeysEqualWithOffKeys(
                                updated_record, sync_record, off_keys
                            )
                            off_keys = PARENT_FIELD_MISSING_SUBFIELDS[stream] | MISSING_FROM_SCHEMA[stream] # BUG_1 | # BUG_2
                            self.assertDictEqualWithOffKeys(
                                updated_record, sync_record, off_keys
                            )  # Test Workaround End ##############################

                        else:
                            self.assertRecordsEqual(stream, updated_record, sync_record)
