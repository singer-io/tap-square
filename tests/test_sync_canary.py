from unittest import TestCase
from base import TestSquareBase, DataType


class TestSyncCanary(TestSquareBase, TestCase):
    """Test that sync code gets exercised for all streams regardless if we can't create data. Validates scopes, authorizations, sync code that can't yet be tested end-to-end."""
    @staticmethod
    def name():
        return "tap_tester_sync_canary"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference({
            'bank_accounts',
            'settlements',
        })

    def testable_streams_static(self):
        return self.static_data_streams()

    def setUp(self):
        super().setUp()
        self.START_DATE = self.get_properties().get('start_date')

    def test_run(self):
        """
        Verify that for each stream you can call sync just so it exercises the code.
        """
        print("\n\nRUNNING {}".format(self.name()))
        print("WITH STREAMS: {}\n\n".format(self.expected_streams()))

        self.set_environment(self.SANDBOX)
        self.run_initial_sync(self.get_environment(), DataType.DYNAMIC)
        self.run_initial_sync(self.get_environment(), DataType.STATIC)

        self.set_environment(self.PRODUCTION)
        self.run_initial_sync(self.get_environment(), DataType.DYNAMIC)
        self.run_initial_sync(self.get_environment(), DataType.STATIC)
