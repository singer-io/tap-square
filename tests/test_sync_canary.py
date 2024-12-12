import tap_tester.connections as connections

from base import TestSquareBaseParent, DataType


class TestSyncCanary(TestSquareBaseParent.TestSquareBase):
    """Test that sync code gets exercised for all streams regardless if we can't create data. Validates scopes, authorizations, sync code that can't yet be tested end-to-end."""

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams()

    def testable_streams_static(self):
        return self.static_data_streams()

    def setUp(self):
        super().setUp()
        self.START_DATE = self.get_properties().get('start_date')

    def run_standard_sync(self, environment, data_type, select_all_fields=True):
        """
        Run the tap in check mode.
        Perform table selection based on testable streams.
        Select all fields or no fields based on the select_all_fields param.
        Run a sync.
        """
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_access_token)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        streams_to_select = self.testable_streams(environment, data_type)

        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(streams_to_select))

        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=select_all_fields
        )

        return self.run_and_verify_sync(conn_id)

    def test_run(self):
        """
        Verify that for each stream you can call sync just so it exercises the code.
        """
        self.set_environment(self.SANDBOX)
        self.run_standard_sync(self.get_environment(), DataType.DYNAMIC)
        self.run_standard_sync(self.get_environment(), DataType.STATIC)

        TestSquareBaseParent.TestSquareBase.test_name = self.prod_test_name
        self.set_environment(self.PRODUCTION)
        self.run_standard_sync(self.get_environment(), DataType.DYNAMIC)
        self.run_standard_sync(self.get_environment(), DataType.STATIC)
        TestSquareBaseParent.TestSquareBase.test_name = self.sandbox_test_name