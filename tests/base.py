import os
import unittest

import tap_tester.connections as connections

class TestSquareBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_SQUARE_REFRESH_TOKEN",
            "TAP_SQUARE_APPLICATION_ID",
            "TAP_SQUARE_APPLICATION_SECRET",
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.square"

    @staticmethod
    def tap_name():
        return "tap-square"

    @staticmethod
    def get_properties():
        return {
            'start_date' : '2020-03-01T00:00:00Z',
            'sandbox' : 'true'
        }

    @staticmethod
    def get_credentials():
        return {
            'refresh_token': os.getenv('TAP_SQUARE_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_SQUARE_APPLICATION_ID'),
            'client_secret': os.getenv('TAP_SQUARE_APPLICATION_SECRET'),
        }

    @staticmethod
    def expected_check_streams():
        return {
            'items',
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "items": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
        }

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    # @staticmethod
    # def preserve_refresh_token(existing_conns, payload):
    #     if not existing_conns:
    #         return payload
    #     conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
    #     payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
    #     return payload
