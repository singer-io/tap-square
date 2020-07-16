import os
import json
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie as menagerie
import tap_tester.connections as connections

from test_client import TestClient


class TestSquareBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    STATIC_START_DATE = "2020-07-13T00:00:00Z"
    START_DATE = ""

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

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()

    def get_properties(self, original = True):
        # Default values
        return_value = {
            'start_date' : dt.strftime(dt.utcnow()-timedelta(days=3), self.START_DATE_FORMAT),
            'sandbox' : 'true'
        }

        if original:
            return return_value

        return_value['start_date'] = self.START_DATE
        return return_value

    @staticmethod
    def get_credentials():
        return {
            'refresh_token': os.getenv('TAP_SQUARE_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_SQUARE_APPLICATION_ID'),
            'client_secret': os.getenv('TAP_SQUARE_APPLICATION_SECRET'),
        }

    def expected_check_streams(self):
        return set(self.expected_metadata().keys()).difference(set())

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "items": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "categories": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "discounts": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "taxes": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "employees": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "locations": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "refunds": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'created_at'}
            },
            "payments": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "modifier_lists": {
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


    def static_data_streams(self):
        """
        Some streams require use of a static data set, and should
        only be referenced in static tests.
        """
        return {
            'locations',  # Limit 300 objects, DELETES not supported
        }

    def dynamic_data_streams(self):
        """Expected streams minus streams with static data."""
        return self.expected_streams().difference(self.static_data_streams())

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_incremental_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.INCREMENTAL)

    def expected_full_table_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.FULL)

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True, exclude_streams=[]):
        """Select all streams and all fields within streams"""

        for catalog in catalogs:
            if exclude_streams and catalog.get('stream_name') in exclude_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )

    def get_selected_fields_from_metadata(self, metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (field['metadata']['inclusion'] == 'automatic'
                                               or field['metadata']['selected'] == True)
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def _get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schemas(self, stream):
        schemas = {}

        path = self._get_abs_path("schemas") + "/" + stream + ".json"
        final_path = path.replace('tests', 'tap_square')

        with open(final_path) as file:
            schemas[stream] = json.load(file)

        return schemas

    def parse_date(self, date_value):
        try:
            date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S.%fZ")
            return date_stripped
        except ValueError:
            try:
                date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%SZ")
                return date_stripped
            except ValueError:
                return date_value

    def expected_schema_keys(self, stream):

        props = self._load_schemas(stream).get(stream).get('properties')
        assert props, "{} schema not configured proprerly"

        return props.keys()

    def sort_records_recur(self, records):
        for record in records:
            self.sort_record_recur(record)

    def sort_record_recur(self, record):
        if isinstance(record, dict):
            for key, value in record.items():
                if type(value) == dict:
                    self.sort_record_recur(value)
                elif type(value) == list:
                    for rec in value:
                        self.sort_record_recur(rec)
                    self.sort_array_type(record, key, value)
            

    def modify_expected_records(self, records):
       for rec in records:
           self.modify_expected_record(rec)

           
    def modify_expected_record(self, expected_record):
        """ Align expected data with how the tap _should_ emit them. """
        if isinstance(expected_record, dict):
            for key, value in expected_record.items(): # Modify a single record
                if type(value) == dict:
                    self.modify_expected_record(value)
                elif type(value) == list:
                    for item in value:
                        self.modify_expected_record(item)
                    self.sort_array_type(expected_record, key, value)
                else:
                    self.align_date_type(expected_record, key, value)

    # def modify_expected_record(self, expected_record):
    #     if type(expected_records) == list: # Modify a list of records
    #         for record in expected_records:
    #             for key, value in record.items():
    #                 self.align_date_type(record, key, value)
    #                 self.sort_array_type(record, key, value)
    #         return

    #     for key, value in expected_records.items(): # Modify a single record
    #         self.align_date_type(expected_records, key, value)
    #         self.sort_array_type(expected_records, key, value)

    def align_date_type(self, record, key, value):
        """datetime values must conform to ISO-8601 or they will be rejected by the gate"""
        if isinstance(value, str) and isinstance(self.parse_date(value), dt):
            # key in ['updated_at', 'created_at']:
            raw_date = self.parse_date(value)
            iso_date = dt.strftime(raw_date,  "%Y-%m-%dT%H:%M:%S.%fZ")
            record[key] = iso_date

    def sort_array_type(self, record, key, value):
        """
        List values are returned by square as unordered arrays.
        In order to accurately compare expected and actual records, we must sort all lists.
        """
        try:
            if isinstance(value, list) and value:
                if isinstance(value[0], dict) and "id" in value[0].keys():
                    record[key] = sorted(value, key=lambda x: x['id'])
                else:
                    record[key] = sorted(value)
        except Exception as ex:
            print("Could not sort array at key: {}, value: {}".format(key, value))
            raise
