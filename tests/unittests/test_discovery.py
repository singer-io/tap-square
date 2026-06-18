import unittest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_square.discover import discover, _apply_access_checks, get_schemas, PRODUCTION_ONLY_STREAMS
from tap_square.client import SquareForbiddenError
from tap_square.streams import STREAMS


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks()."""

    def _make_client(self):
        return MagicMock()

    def _make_schemas(self, sandbox=False):
        schemas, schemas_metadata = get_schemas(sandbox=sandbox)
        return schemas, schemas_metadata

    @patch.object(
        __import__('tap_square.streams', fromlist=['Stream']).Stream,
        'check_access',
        return_value=True,
    )
    def test_all_streams_accessible_leaves_catalog_unchanged(self, _mock):
        schemas, schemas_metadata = self._make_schemas()
        original_keys = set(schemas.keys())
        _apply_access_checks(self._make_client(), schemas, schemas_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_inaccessible_streams_removed_from_schemas(self):
        schemas, schemas_metadata = self._make_schemas()
        client = self._make_client()

        # Mark 'items' and 'categories' as inaccessible
        def fake_check_access(stream_instance):
            return stream_instance.tap_stream_id not in ('items', 'categories')

        with patch.dict(
            {name: type(cls.__name__, (cls,), {'check_access': lambda self: fake_check_access(self)})
             for name, cls in STREAMS.items()},
        ):
            # Simpler approach: patch check_access on the stream objects
            pass

        # Direct approach using side_effect on instantiated streams
        originals = {}
        for name, cls in STREAMS.items():
            orig = cls.check_access
            originals[name] = orig
            if name in ('items', 'categories'):
                cls.check_access = lambda self: False
            else:
                cls.check_access = lambda self: True

        try:
            _apply_access_checks(client, schemas, schemas_metadata)
            self.assertNotIn('items', schemas)
            self.assertNotIn('categories', schemas)
            # All other streams should still be present
            for name in STREAMS:
                if name not in ('items', 'categories'):
                    self.assertIn(name, schemas)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_raises_forbidden_when_no_streams_accessible(self):
        schemas, schemas_metadata = self._make_schemas()
        client = self._make_client()

        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: False

        try:
            with self.assertRaises(SquareForbiddenError) as ctx:
                _apply_access_checks(client, schemas, schemas_metadata)
            self.assertIn('403', str(ctx.exception))
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_schemas_metadata_stays_consistent_with_schemas(self):
        schemas, schemas_metadata = self._make_schemas()
        client = self._make_client()

        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = (lambda n: lambda self: n != 'items')(name)

        try:
            _apply_access_checks(client, schemas, schemas_metadata)
            self.assertEqual(set(schemas.keys()), set(schemas_metadata.keys()))
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]


class TestDiscover(unittest.TestCase):
    """Integration tests for discover()."""

    def _make_client(self):
        return MagicMock()

    def test_discover_returns_catalog(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: True

        try:
            catalog = discover(client, sandbox=False)
            self.assertIsInstance(catalog, Catalog)
            self.assertGreater(len(catalog.streams), 0)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_discover_sandbox_excludes_production_only_streams(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: True

        try:
            catalog = discover(client, sandbox=True)
            stream_ids = {s.tap_stream_id for s in catalog.streams}
            for prod_stream in PRODUCTION_ONLY_STREAMS:
                self.assertNotIn(prod_stream, stream_ids)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_discover_non_sandbox_includes_production_streams(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: True

        try:
            catalog = discover(client, sandbox=False)
            stream_ids = {s.tap_stream_id for s in catalog.streams}
            for prod_stream in PRODUCTION_ONLY_STREAMS:
                self.assertIn(prod_stream, stream_ids)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_discover_excludes_forbidden_streams(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = (lambda n: lambda self: n != 'items')(name)

        try:
            catalog = discover(client, sandbox=False)
            stream_ids = {s.tap_stream_id for s in catalog.streams}
            self.assertNotIn('items', stream_ids)
            self.assertIn('categories', stream_ids)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_discover_raises_when_all_streams_forbidden(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: False

        try:
            with self.assertRaises(SquareForbiddenError):
                discover(client, sandbox=False)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]

    def test_discover_catalog_entry_has_required_fields(self):
        client = self._make_client()
        originals = {}
        for name, cls in STREAMS.items():
            originals[name] = cls.check_access
            cls.check_access = lambda self: True

        try:
            catalog = discover(client, sandbox=False)
            for stream in catalog.streams:
                self.assertIsNotNone(stream.stream)
                self.assertIsNotNone(stream.tap_stream_id)
                self.assertIsNotNone(stream.schema)
                self.assertIsNotNone(stream.metadata)
        finally:
            for name, cls in STREAMS.items():
                cls.check_access = originals[name]


class TestCheckAccess(unittest.TestCase):
    """Unit tests for check_access() on individual stream classes."""

    def _make_client(self):
        return MagicMock()

    def test_check_access_returns_true_when_catalog_stream_probe_succeeds(self):
        from tap_square.streams import Items
        client = self._make_client()
        client.get_catalog.return_value = iter([([], None)])
        stream = Items(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_returns_false_when_catalog_stream_probe_forbidden(self):
        from tap_square.streams import Items
        client = self._make_client()
        client.get_catalog.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Items(client=client)
        self.assertFalse(stream.check_access())

    def test_check_access_returns_true_when_locations_probe_succeeds(self):
        from tap_square.streams import Locations
        client = self._make_client()
        client.get_locations.return_value = iter([([], None)])
        stream = Locations(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_returns_false_when_locations_probe_forbidden(self):
        from tap_square.streams import Locations
        client = self._make_client()
        client.get_locations.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Locations(client=client)
        self.assertFalse(stream.check_access())

    def test_check_access_returns_true_when_customers_probe_succeeds(self):
        from tap_square.streams import Customers
        client = self._make_client()
        client.get_customers.return_value = iter([([], None)])
        stream = Customers(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_returns_false_when_customers_probe_forbidden(self):
        from tap_square.streams import Customers
        client = self._make_client()
        client.get_customers.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Customers(client=client)
        self.assertFalse(stream.check_access())

    def test_base_stream_check_access_returns_true_by_default(self):
        from tap_square.streams import Stream
        client = self._make_client()
        stream = Stream(client=client)
        self.assertTrue(stream.check_access())

    def test_payments_check_access_returns_true_when_probe_succeeds(self):
        from tap_square.streams import Payments
        client = self._make_client()
        client.get_locations.return_value = iter([([{'id': 'loc1'}], None)])
        client.get_payments.return_value = iter([([], None)])
        stream = Payments(client=client)
        self.assertTrue(stream.check_access())

    def test_payments_check_access_returns_false_when_payments_forbidden(self):
        from tap_square.streams import Payments
        client = self._make_client()
        client.get_locations.return_value = iter([([{'id': 'loc1'}], None)])
        client.get_payments.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Payments(client=client)
        self.assertFalse(stream.check_access())

    def test_payments_check_access_skips_probe_when_locations_forbidden(self):
        """Payments probe must be independent: if locations returns 403, skip probe and return True."""
        from tap_square.streams import Payments
        client = self._make_client()
        client.get_locations.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Payments(client=client)
        self.assertTrue(stream.check_access())
        client.get_payments.assert_not_called()

    def test_orders_check_access_returns_false_when_orders_forbidden(self):
        from tap_square.streams import Orders
        client = self._make_client()
        client.get_locations.return_value = iter([([{'id': 'loc1'}], None)])
        client.get_orders.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Orders(client=client)
        self.assertFalse(stream.check_access())

    def test_orders_check_access_skips_probe_when_locations_forbidden(self):
        """Orders probe must be independent: if locations returns 403, skip probe and return True."""
        from tap_square.streams import Orders
        client = self._make_client()
        client.get_locations.side_effect = SquareForbiddenError("403 Forbidden")
        stream = Orders(client=client)
        self.assertTrue(stream.check_access())
        client.get_orders.assert_not_called()

    def test_team_members_check_access_returns_false_when_forbidden(self):
        from tap_square.streams import TeamMembers
        client = self._make_client()
        client.get_locations.return_value = iter([([{'id': 'loc1'}], None)])
        client.get_team_members.side_effect = SquareForbiddenError("403 Forbidden")
        stream = TeamMembers(client=client)
        self.assertFalse(stream.check_access())

    def test_team_members_check_access_still_probes_with_empty_locations(self):
        """TeamMembers accepts an empty location list, so the probe still runs even when locations is forbidden."""
        from tap_square.streams import TeamMembers
        client = self._make_client()
        client.get_locations.side_effect = SquareForbiddenError("403 Forbidden")
        client.get_team_members.return_value = iter([([], None)])
        stream = TeamMembers(client=client)
        self.assertTrue(stream.check_access())
        client.get_team_members.assert_called_once_with([])


if __name__ == '__main__':
    unittest.main()
