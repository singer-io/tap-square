import unittest
from unittest.mock import patch
from singer import Transformer

from tap_square.client import SquareClient
from tap_square.streams import TeamMembers


mock_config = {
    "sandbox": "true",
    "start_date": "2023-07-11T00:00:00Z",
    "refresh_token": "123456789",
    "client_id": "abcdefgh",
    "client_secret": "abc1234",
}

expected_return_state = {
    "currently_syncing": "team_members",
    "bookmarks": {"team_members": {"updated_at": "2023-07-11T00:00:00Z"}},
}

mock_response_location_ids = [
    (
        [
            {
                "id": "location_1",
                "name": "Default Test Account 1",
            },
            {
                "id": "location_2",
                "name": "Default Test Account 2",
            },
        ],
        None,
    )
]

mock_response_data = [
    (
        [
            {
                "id": "F-2TzqyIydk0gAtzGqZT",
                "is_owner": True,
                "status": "ACTIVE",
                "given_name": "Sandbox",
                "family_name": "Seller",
                "email_address": "sandbox-merchant+d8ulrxk1tzu7fbdjmravimc7b8tbbd0a@squareup.com",
                "created_at": "2020-07-10T13:58:15Z",
                "updated_at": "2023-07-07T16:38:20Z",
                "assigned_locations": {
                    "assignment_type": "ALL_CURRENT_AND_FUTURE_LOCATIONS"
                },
            }
        ],
        None,
    )
]

stream_schema = {
    "properties": {
        "id": {"type": ["null", "string"]},
        "reference_id": {"type": ["null", "string"]},
        "is_owner": {"type": ["null", "boolean"]},
        "status": {"type": ["null", "string"]},
        "given_name": {"type": ["null", "string"]},
        "family_name": {"type": ["null", "string"]},
        "email_address": {"type": ["null", "string"]},
        "phone_number": {"type": ["null", "string"]},
        "created_at": {"format": "date-time", "type": ["null", "string"]},
        "updated_at": {"format": "date-time", "type": ["null", "string"]},
        "assigned_locations": {
            "properties": {
                "assignment_type": {"type": ["null", "string"]},
                "location_ids": {
                    "items": {"type": ["null", "string"]},
                    "type": ["null", "array"],
                },
            },
            "type": ["null", "object"],
        },
    },
    "type": ["null", "object"],
}


stream_metadata = {
    (): {
        "table-key-properties": ["id"],
        "forced-replication-method": "INCREMENTAL",
        "selected": True,
        "inclusion": "available",
        "valid-replication-keys": ["updated_at"],
    },
    ("properties", "id"): {"inclusion": "automatic"},
    ("properties", "reference_id"): {"inclusion": "available"},
    ("properties", "is_owner"): {"inclusion": "available"},
    ("properties", "status"): {"inclusion": "available"},
    ("properties", "given_name"): {"inclusion": "available"},
    ("properties", "family_name"): {"inclusion": "available"},
    ("properties", "email_address"): {"inclusion": "available"},
    ("properties", "phone_number"): {"inclusion": "available"},
    ("properties", "created_at"): {"inclusion": "available"},
    ("properties", "updated_at"): {"inclusion": "automatic"},
    ("properties", "assigned_locations"): {"inclusion": "available"},
}


class TestTeamMembers(unittest.TestCase):
    @patch("tap_square.client.SquareClient.get_locations", return_value=mock_response_location_ids,)
    @patch("tap_square.client.SquareClient._get_v2_objects", return_value=mock_response_data,)
    @patch("tap_square.client.SquareClient._get_access_token", return_value= "mock_token")
    def test_search_team_members_sync(self, mocked_access_token, mocked_get_v2_objects, mocked_get_locations):
        """
        # Validating the state returned while making the API request to search_team_members
        """
        expected_return_value = expected_return_state

        team_members_obj = TeamMembers(SquareClient(mock_config, 'config_path'))
        return_value = team_members_obj.sync(
            {"currently_syncing": "team_members"},
            stream_schema,
            stream_metadata,
            mock_config,
            Transformer(),
        )

        self.assertEqual(expected_return_value, return_value)
