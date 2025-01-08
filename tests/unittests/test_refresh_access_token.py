import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from singer import utils
from tap_square.client import require_new_access_token, SquareClient

REFRESH_TOKEN_BEFORE = 22


class TestRequireNewAccessToken(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.access_token = "test_access_token"

    def test_no_access_token(self):
        '''Return true when no access token is provided.'''
        result = require_new_access_token(None, self.client)
        self.assertTrue(result)

    @patch("singer.http_request_timer")
    def test_valid_access_token(self, mock_timer):
        '''Return false when the token generated in less than 7 days.'''
        token_expiry = utils.strftime(utils.now() + timedelta(days=26))
        self.client.o_auth.retrieve_token_status.return_value = MagicMock(
            is_error=MagicMock(return_value=False),
            body={"expires_at": token_expiry},
        )
        result = require_new_access_token(self.access_token, self.client)
        self.assertFalse(result)

    @patch("singer.http_request_timer")
    def test_access_token_older_than_7_days(self, mock_timer):
        '''Return true when the token is older than 7 days.'''
        token_expiry = utils.strftime(utils.now() + timedelta(days=20))
        self.client.o_auth.retrieve_token_status.return_value = MagicMock(
            is_error=MagicMock(return_value=False),
            body={"expires_at": token_expiry},
        )
        result = require_new_access_token(self.access_token, self.client)
        self.assertTrue(result)

    @patch("singer.http_request_timer")
    def test_almost_expired_access_token(self, mock_timer):
        '''Return false when the token is created exactly 7 days ago'''
        token_expiry = utils.strftime(utils.now() + timedelta(days=24))
        self.client.o_auth.retrieve_token_status.return_value = MagicMock(
            is_error=MagicMock(return_value=False),
            body={"expires_at": token_expiry},
        )
        result = require_new_access_token(self.access_token, self.client)
        self.assertFalse(result)

    @patch("singer.http_request_timer")
    def test_api_error(self, mock_timer):
        '''Return false when the API returns an error.'''
        self.client.o_auth.retrieve_token_status.return_value = MagicMock(
            is_error=MagicMock(return_value=True), errors="API error"
        )
        result = require_new_access_token(self.access_token, self.client)
        self.assertTrue(result)


class TestGetAccessToken(unittest.TestCase):
    def setUp(self):
        self.config_path = "/path/to/config.json"

        self.config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "environment": "sandbox",
            "access_token": "cached_token",
        }

    @patch("tap_square.client.require_new_access_token")
    @patch("tap_square.client.write_config")
    @patch("singer.http_request_timer")
    def test_get_access_token_no_refresh_needed(
        self, mock_http_timer, mock_write_config, mock_require_new_access_token
    ):
        '''
        Test the case where the access token does not need to be refreshed
        '''
        mock_require_new_access_token.return_value = False

        _instance = SquareClient(self.config, self.config_path)

        # Assertions
        self.assertEqual(_instance._access_token, "cached_token")
        mock_write_config.assert_not_called()

    @patch("tap_square.client.Client")
    @patch("tap_square.client.require_new_access_token")
    @patch("tap_square.client.write_config")
    @patch("singer.http_request_timer")
    def test_get_access_token_refresh_needed_success(
        self,
        mock_http_timer,
        mock_write_config,
        mock_require_new_access_token,
        mock_client,
    ):
        '''
        Test the case where the access token needs to be refreshed and the API returns a new token
        '''
        mock_require_new_access_token.return_value = True

        # Mock the Client's o_auth.obtain_token method
        mock_client_instance = mock_client.return_value
        mock_client_instance.o_auth.obtain_token.return_value = MagicMock(
            is_error=MagicMock(return_value=False),
            body={"access_token": "new_token", "refresh_token": "new_refresh_token"},
        )

        _instance = SquareClient(self.config, self.config_path)

        # Assertions
        self.assertEqual(_instance._access_token, "new_token")
        mock_write_config.assert_called_once_with(
            {
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "refresh_token": "test_refresh_token",
                "environment": "sandbox",
                "access_token": "cached_token",
            },
            "/path/to/config.json",
            {"access_token": "new_token", "refresh_token": "new_refresh_token"},
        )
        mock_client_instance.o_auth.obtain_token.assert_called_once()

    @patch("tap_square.client.Client")
    @patch("tap_square.client.require_new_access_token")
    @patch("singer.http_request_timer")
    def test_get_access_token_refresh_needed_error(
        self, mock_http_timer, mock_require_new_access_token, mock_client
    ):
        '''
        Test the case where the API returns an error while refreshing the access token
        '''
        mock_require_new_access_token.return_value = True

        # Mock the Client's o_auth.obtain_token method to return an error
        mock_client_instance = mock_client.return_value
        mock_client_instance.o_auth.obtain_token.return_value = MagicMock(
            is_error=MagicMock(return_value=True), errors=["Invalid credentials"]
        )

        # Call the method and check for exception
        with self.assertRaises(RuntimeError) as context:
            SquareClient(self.config, self.config_path)

        self.assertIn("Invalid credentials", str(context.exception))
        mock_client_instance.o_auth.obtain_token.assert_called_once()
