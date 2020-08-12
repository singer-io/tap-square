from base import TestSquareBase, DataType


class TestSyncCanary(TestSquareBase):
    """Test that sync code gets exercised for all streams regardless if we can't create data. Validates scopes, authorizations, sync code that can't yet be tested end-to-end."""
    @staticmethod
    def name():
        return "tap_tester_sync_canary"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams()

    def testable_streams_static(self):
        return self.static_data_streams()

    def setup(self):
        super().setup()
        self.START_DATE = self.get_properties().get('start_date')

        # All streams should work in production, even though some don't work in sandbox
        self.set_environment(self.PRODUCTION)

    def test_run(self):
        """
        Verify that for each stream you can call sync just so it exercises the code.
        """
        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.expected_streams()))

        self.run_initial_sync(self.get_environment(), DataType.DYNAMIC)
        self.run_initial_sync(self.get_environment(), DataType.STATIC)
        
