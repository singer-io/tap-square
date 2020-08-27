import random
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.connections as connections

from base import TestSquareBaseParent, DataType


class TestSquareStartDateDefault(TestSquareBaseParent.TestSquareBase):
    """
    Test we can perform a successful sync for all streams with a start date of 1 year ago and older.
    This makes up for the time partions missed by `test_start_date.py`.
    """
    @staticmethod
    def name():
        return "tap_tester_start_date_default"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference({
            'bank_accounts',
            'settlements',
        })

    def testable_streams_static(self):
        return self.static_data_streams()

    def run_standard_sync(self, environment, data_type, select_all_fields=True):
        """
        Run the tap in check mode.
        Perform table selection based on testable streams.
        Select all fields or no fields based on the select_all_fields param.
        Run a sync.
        """
        conn_id = connections.ensure_connection(self, original_properties=False)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        streams_to_select = self.testable_streams(environment, data_type)

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}".format(streams_to_select))
        print("WITH START DATE: {}\n\n".format(self.START_DATE))

        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=select_all_fields
        )

        return self.run_and_verify_sync(conn_id)

    def test_run(self):
        """
        Verify that for each stream you can call sync just so it exercises the code.
        """
        default_start_date = dt.utcnow() - timedelta(days=365) # 1 year ago

        # Testing Default start date (1 year ago)
        self.START_DATE = dt.strftime(default_start_date, self.START_DATE_FORMAT)
        self.set_environment(self.SANDBOX)
        self.default_start_date_test(DataType.DYNAMIC, self.testable_streams_dynamic().intersection(self.sandbox_streams()))
        self.default_start_date_test(DataType.STATIC, self.testable_streams_static().intersection(self.sandbox_streams()))
        self.set_environment(self.PRODUCTION)
        self.default_start_date_test(DataType.DYNAMIC, self.testable_streams_dynamic().intersection(self.production_streams()))
        self.default_start_date_test(DataType.STATIC, self.testable_streams_static().intersection(self.production_streams()))

        days_prior = random.randint(7, 358) # 1 week to 57 weeks

        # Testing start date prior to default
        self.START_DATE = dt.strftime(default_start_date - timedelta(days=days_prior), self.START_DATE_FORMAT)
        self.set_environment(self.SANDBOX)
        self.default_start_date_test(DataType.DYNAMIC, self.testable_streams_dynamic().intersection(self.sandbox_streams()))
        self.default_start_date_test(DataType.STATIC, self.testable_streams_static().intersection(self.sandbox_streams()))
        self.set_environment(self.PRODUCTION)
        self.default_start_date_test(DataType.DYNAMIC, self.testable_streams_dynamic().intersection(self.production_streams()))
        self.default_start_date_test(DataType.STATIC, self.testable_streams_static().intersection(self.production_streams()))

    def default_start_date_test(self, data_type, testable_streams):
        streams_without_data = self.untestable_streams()

        # Verify we can run a check and sync with the given start date
        record_count_by_stream = self.run_standard_sync(self.get_environment(), data_type)

        # Verify we get records for streams with data
        for stream in testable_streams:
            if stream in streams_without_data:
                self.assertGreaterEqual(record_count_by_stream.get(stream, 0), 0)
            else:
                self.assertGreater(record_count_by_stream.get(stream), 0)
