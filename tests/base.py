import os
import json
from datetime import datetime as dt
from datetime import timedelta
from abc import ABC, abstractmethod
from enum import Enum

import singer

import tap_tester.menagerie as menagerie
import tap_tester.connections as connections
import tap_tester.runner      as runner

from test_client import TestClient

LOGGER = singer.get_logger()


class DataType(Enum):
    DYNAMIC = 1
    STATIC = 2


class TestSquareBase(ABC):
    PRODUCTION = "production"
    SANDBOX = "sandbox"
    SQUARE_ENVIRONMENT = SANDBOX
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    START_DATE_KEY = 'start-date-key'
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

        # Allows diffs in asserts to print more
        self.maxDiff = None
        self.set_environment(self.SANDBOX) # We always want the tests to start in sandbox

    @staticmethod
    def get_type():
        return "platform.square"

    @staticmethod
    def tap_name():
        return "tap-square"

    def set_environment(self, env):
        """
        Change the Square App Environmnet.
        Requires re-instatiating TestClient and setting env var.
        """
        os.environ['TAP_SQUARE_ENVIRONMENT'] = env
        self.client = TestClient(env=env)
        self.SQUARE_ENVIRONMENT = env

    @staticmethod
    def get_environment():
        return os.environ['TAP_SQUARE_ENVIRONMENT']

    def get_properties(self, original=True):
        # Default values
        return_value = {
            'start_date': dt.strftime(dt.utcnow() - timedelta(days=3), self.START_DATE_FORMAT),
            'sandbox': 'true' if self.get_environment() == self.SANDBOX else 'false'
        }

        if not original:
            return_value['start_date'] = self.START_DATE

        return return_value

    @staticmethod
    def get_credentials():
        environment = os.getenv('TAP_SQUARE_ENVIRONMENT')
        if environment in ['sandbox', 'production']:
            creds = {
                'refresh_token': os.getenv('TAP_SQUARE_REFRESH_TOKEN') if environment == 'sandbox' else os.getenv('TAP_SQUARE_PROD_REFRESH_TOKEN'),
                'client_id': os.getenv('TAP_SQUARE_APPLICATION_ID') if environment == 'sandbox' else os.getenv('TAP_SQUARE_PROD_APPLICATION_ID'),
                'client_secret': os.getenv('TAP_SQUARE_APPLICATION_SECRET') if environment == 'sandbox' else os.getenv('TAP_SQUARE_PROD_APPLICATION_SECRET'),
            }
        else:
            raise Exception("Square Environment: {} is not supported.".format(environment))

        return creds

    def expected_check_streams(self):
        return set(self.expected_metadata().keys()).difference(set())

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "bank_accounts": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "cash_drawer_shifts": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
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
            "employees": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
                self.START_DATE_KEY: 'updated_at'
            },
            "inventories": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.FULL,
                self.START_DATE_KEY: 'calculated_at',
            },
            "items": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "locations": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "modifier_lists": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "orders": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "payments": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "refunds": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "roles": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
                self.START_DATE_KEY: 'updated_at'
            },
            "settlements": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "shifts": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            "taxes": {
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

    @staticmethod
    def production_streams():
        """Some streams can only have data on the production app. We must test these separately"""
        return {
            'employees',
            'roles',
        }

    def sandbox_streams(self):
        """By default we will be testing streams in the sandbox"""
        return self.expected_streams().difference(self.production_streams())

    @staticmethod
    def static_data_streams():
        """
        Some streams require use of a static data set, and should
        only be referenced in static tests.
        """
        return {
            'locations',  # Limit 300 objects, DELETES not supported
        }

    def untestable_streams(self):
        """STREAMS THAT CANNOT CURRENTLY BE TESTED"""
        return {
            'bank_accounts',  # No endpoints for CREATE or UPDATE
            'cash_drawer_shifts',  # Require cash transactions (not supported by API)
            'settlements',  # Depenedent on bank_account related transactions, no endpoints for CREATE or UPDATE
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

    @abstractmethod
    def testable_streams_dynamic(self):
        pass

    @abstractmethod
    def testable_streams_static(self):
        pass

    def testable_streams(self, environment='sandbox', data_type=DataType.DYNAMIC):
        allowed_environments = {self.SANDBOX, self.PRODUCTION}
        if environment not in allowed_environments:
            raise NotImplementedError("Environment can only be one of {}, but {} provided".format(allowed_environments, environment))

        if environment == self.SANDBOX:
            if data_type == DataType.DYNAMIC:
                return self.testable_streams_dynamic().intersection(self.sandbox_streams())
            else:
                return self.testable_streams_static().intersection(self.sandbox_streams())
        else:
            if data_type == DataType.DYNAMIC:
                return self.testable_streams_dynamic().intersection(self.production_streams())
            else:
                return self.testable_streams_static().intersection(self.production_streams())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def makeshift_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {
            'inventories': {'catalog_object_id', 'location_id', 'state'}
        }

    def expected_replication_keys(self):
        incremental_streams = self.expected_incremental_streams()
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items() if table in incremental_streams}

    def expected_stream_to_start_date_key(self):
        return {table: properties.get(self.START_DATE_KEY)
                for table, properties in self.expected_metadata().items()
                if properties.get(self.START_DATE_KEY)}

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True, exclude_streams=None):
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

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (field['metadata']['inclusion'] == 'automatic'
                                               or field['metadata']['selected'] is True)
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    @staticmethod
    def _get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schemas(self, stream):
        schemas = {}

        path = self._get_abs_path("schemas") + "/" + stream + ".json"
        final_path = path.replace('tests', 'tap_square')

        with open(final_path) as file:
            schemas[stream] = json.load(file)

        return schemas

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        try:
            date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S.%fZ")
            return date_stripped
        except ValueError:
            try:
                date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%SZ")
                return date_stripped
            except ValueError:
                raise NotImplementedError("We are not accounting for dates of this format: {}".format(date_value))

    @staticmethod
    def date_check_and_parse(date_value):
        """
        Pass in any value and return that value. If the value is a string-formatted-datetime, parse
        the value and return it as an unformatted datetime object.
        """
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

    def modify_expected_records(self, records):
        for rec in records:
            self.modify_expected_record(rec)

    def modify_expected_record(self, expected_record):
        """ Align expected data with how the tap _should_ emit them. """
        if isinstance(expected_record, dict):
            for key, value in expected_record.items(): # Modify a single record
                if isinstance(value, dict):
                    self.modify_expected_record(value)
                elif isinstance(value, list):
                    for item in value:
                        self.modify_expected_record(item)
                else:
                    self.align_date_type(expected_record, key, value)
                    self.align_number_type(expected_record, key, value)

    def align_date_type(self, record, key, value):
        """datetime values must conform to ISO-8601 or they will be rejected by the gate"""
        if isinstance(value, str) and isinstance(self.date_check_and_parse(value), dt):
            raw_date = self.date_check_and_parse(value)
            iso_date = dt.strftime(raw_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            record[key] = iso_date

    @staticmethod
    def align_number_type(record, key, value):
        """float values must conform to json number formatting so we convert to Decimal"""
        if isinstance(value, float) and key in ['latitude', 'longitude']:
            record[key] = str(value)

    def create_test_data(self, testable_streams, start_date, start_date_2=None, min_required_num_records_per_stream=None, force_create_records=False):
        if min_required_num_records_per_stream is None:
            min_required_num_records_per_stream = {
                stream: 1
                for stream in testable_streams
            }

        if not start_date_2:
            start_date_2 = start_date

        # Force modifier_lists to go first and payments to go last
        create_test_data_streams = list(testable_streams)
        create_test_data_streams = self._shift_to_start_of_list('employees', create_test_data_streams)
        create_test_data_streams = self._shift_to_start_of_list('modifier_lists', create_test_data_streams)
        if 'payments' in testable_streams:
            create_test_data_streams.remove('payments')
            create_test_data_streams.append('payments')

        expected_records = {stream: [] for stream in self.expected_streams()}

        for stream in create_test_data_streams:
            expected_records[stream] = self.client.get_all(stream, start_date)

            start_date_key = self.get_start_date_key(stream)
            if (not any([stream_obj.get(start_date_key) and self.parse_date(stream_obj.get(start_date_key)) > self.parse_date(start_date_2)
                        for stream_obj in expected_records[stream]])
                    or len(expected_records[stream]) <= min_required_num_records_per_stream[stream]
                    or force_create_records):

                num_records = max(1, min_required_num_records_per_stream[stream] + 1 - len(expected_records[stream]))

                LOGGER.info("Data missing for stream %s, will create %s record(s)", stream, num_records)
                created_records = self.client.create(stream, start_date=start_date, num_records=num_records)

                if isinstance(created_records, dict):
                    expected_records[stream].append(created_records)
                elif isinstance(created_records, list):
                    expected_records[stream].extend(created_records)
                else:
                    raise NotImplementedError("created_records unknown type: {}".format(created_records))

            print("Adjust expectations for stream: {}".format(stream))
            self.modify_expected_records(expected_records[stream])

        return expected_records

    def get_start_date_key(self, stream):
        replication_type = self.expected_replication_method().get(stream)
        if replication_type == self.INCREMENTAL and self.expected_replication_keys().get(stream):
            start_date_key = next(iter(self.expected_replication_keys().get(stream)))
        elif replication_type == self.FULL and self.expected_stream_to_start_date_key().get(stream):
            start_date_key = self.expected_stream_to_start_date_key().get(stream)
        else:
            start_date_key = 'created_at'

        return start_date_key

    @staticmethod
    def _shift_to_start_of_list(key, values):
        new_list = values.copy()

        if key in values:
            new_list.remove(key)
            new_list.insert(0, key)

        return new_list

    def run_initial_sync(self, environment, data_type, select_all_fields=True):
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
        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # Select all available fields from all testable streams
        exclude_streams = self.expected_streams().difference(self.testable_streams(environment, data_type))
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=select_all_fields, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in self.testable_streams(environment, data_type):
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
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

        return conn_id, first_record_count_by_stream

    def assertRecordsEqualByPK(self, stream, expected_records, actual_records):
        """Compare expected and actual records by their primary key."""

        primary_keys = list(self.expected_primary_keys().get(stream)) if self.expected_primary_keys().get(stream) else self.makeshift_primary_keys().get(stream)

        # Verify there are no duplicate pks in the target
        actual_pks = [tuple(actual_record.get(pk) for pk in primary_keys) for actual_record in actual_records]
        actual_pks_set = set(actual_pks)
        self.assertEqual(len(actual_pks), len(actual_pks_set), msg="A duplicate record may have been replicated.")
        actual_pks_to_record_dict = {tuple(actual_record.get(pk) for pk in primary_keys): actual_record for actual_record in actual_records}

        # Verify there are no duplicate pks in our expectations
        expected_pks = [tuple(expected_record.get(pk) for pk in primary_keys) for expected_record in expected_records]
        expected_pks_set = set(expected_pks)
        self.assertEqual(len(expected_pks), len(expected_pks_set), msg="Our expectations contain a duplicate record.")
        expected_pks_to_record_dict = {tuple(expected_record.get(pk) for pk in primary_keys): expected_record for expected_record in expected_records}

        # Verify that all expected records and ONLY the expected records were replicated
        self.assertEqual(expected_pks_set, actual_pks_set)

        return expected_pks_to_record_dict, actual_pks_to_record_dict
