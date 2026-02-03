from unittest.mock import Mock, patch

import pytest
import requests
import responses
from clients.authentication_client import AuthenticationClient


class TestAuthenticationClient:
    """Tests for AuthenticationClient class."""

    def test_initialization(self, mock_session_manager):
        """Should initialize and register with session manager."""
        client = AuthenticationClient(mock_session_manager)
        mock_session_manager.set_auth_client.assert_called_once_with(client)

    @responses.activate
    def test_authenticate_success(self, mock_session_manager):
        """Should authenticate successfully and return token."""
        # Mock the cognito-config endpoint
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/authentication/cognito-config",
            json={
                "tokenPool": {"appClientId": "test-client-id"},
                "region": "us-east-1",
            },
            status=200,
        )

        with patch("clients.authentication_client.boto3") as mock_boto3:
            mock_cognito = Mock()
            mock_boto3.client.return_value = mock_cognito
            mock_cognito.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "test-access-token"}}

            client = AuthenticationClient(mock_session_manager)
            result = client.authenticate()

            assert result == "test-access-token"
            assert mock_session_manager.session_token == "test-access-token"

            # Verify boto3 client was created with fetched config
            mock_boto3.client.assert_called_once_with(
                "cognito-idp",
                region_name="us-east-1",
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            mock_cognito.initiate_auth.assert_called_once_with(
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": mock_session_manager.api_key,
                    "PASSWORD": mock_session_manager.api_secret,
                },
                ClientId="test-client-id",
            )

    @responses.activate
    def test_authenticate_http_error(self, mock_session_manager):
        """Should raise exception when cognito-config request fails."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/authentication/cognito-config",
            status=500,
        )

        client = AuthenticationClient(mock_session_manager)

        with pytest.raises(requests.HTTPError):
            client.authenticate()

    @responses.activate
    def test_authenticate_cognito_failure(self, mock_session_manager):
        """Should raise exception on Cognito authentication failure."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/authentication/cognito-config",
            json={
                "tokenPool": {"appClientId": "test-client-id"},
                "region": "us-east-1",
            },
            status=200,
        )

        with patch("clients.authentication_client.boto3") as mock_boto3:
            mock_cognito = Mock()
            mock_boto3.client.return_value = mock_cognito
            mock_cognito.initiate_auth.side_effect = Exception("Invalid credentials")

            client = AuthenticationClient(mock_session_manager)

            with pytest.raises(Exception, match="Invalid credentials"):
                client.authenticate()
