import base64
import json
from unittest.mock import Mock, patch

import pytest
import requests
import responses

from processor.clients.authentication_client import (
    CognitoClient,
    KeySecretAuthProvider,
    TokenAuthProvider,
)


def _make_jwt(payload: dict) -> str:
    """Build a fake JWT with the given payload (no signature verification)."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.fake-signature"


def _add_cognito_config_response(
    api_host: str = "https://api.test.com",
    app_client_id: str = "test-client-id",
    region: str = "us-east-1",
):
    responses.add(
        responses.GET,
        f"{api_host}/authentication/cognito-config",
        json={"tokenPool": {"appClientId": app_client_id}, "region": region},
        status=200,
    )


class TestCognitoClient:
    """Tests for shared CognitoClient logic."""

    def test_initialization(self):
        client = CognitoClient("https://api.test.com")
        assert client.api_host == "https://api.test.com"
        assert client._cognito_config is None

    @responses.activate
    def test_authenticate_success(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {
            "AuthenticationResult": {
                "AccessToken": "test-access-token",
                "RefreshToken": "test-refresh-token",
            }
        }

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            client = CognitoClient("https://api.test.com")
            access_token, refresh_token = client.authenticate("api-key", "api-secret")

        assert access_token == "test-access-token"
        assert refresh_token == "test-refresh-token"

    @responses.activate
    def test_authenticate_calls_cognito_with_correct_params(self):
        _add_cognito_config_response(app_client_id="my-app-client-id", region="us-west-2")

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {
            "AuthenticationResult": {"AccessToken": "token", "RefreshToken": "refresh"}
        }

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp) as mock_boto:
            client = CognitoClient("https://api.test.com")
            client.authenticate("my-api-key", "my-api-secret")

        mock_boto.assert_called_once_with(
            "cognito-idp",
            region_name="us-west-2",
            aws_access_key_id="",
            aws_secret_access_key="",
        )
        mock_idp.initiate_auth.assert_called_once_with(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "my-api-key", "PASSWORD": "my-api-secret"},
            ClientId="my-app-client-id",
        )

    @responses.activate
    def test_authenticate_raises_on_config_http_error(self):
        responses.add(
            responses.GET,
            "https://api.test.com/authentication/cognito-config",
            json={"error": "Server error"},
            status=500,
        )

        client = CognitoClient("https://api.test.com")
        with pytest.raises(requests.HTTPError):
            client.authenticate("key", "secret")

    @responses.activate
    def test_refresh_token_success(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "new-access-token"}}

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            client = CognitoClient("https://api.test.com")
            new_token = client.refresh_token("refresh-token-value")

        assert new_token == "new-access-token"
        mock_idp.initiate_auth.assert_called_once_with(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": "refresh-token-value"},
            ClientId="test-client-id",
        )

    @responses.activate
    def test_refresh_token_includes_device_key_when_present(self):
        _add_cognito_config_response()

        session_token = _make_jwt({"sub": "user-123", "device_key": "device-abc"})

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "new-access-token"}}

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            client = CognitoClient("https://api.test.com")
            client.refresh_token("refresh-token-value", session_token=session_token)

        mock_idp.initiate_auth.assert_called_once_with(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": "refresh-token-value", "DEVICE_KEY": "device-abc"},
            ClientId="test-client-id",
        )

    @responses.activate
    def test_refresh_token_omits_device_key_when_session_token_undecodable(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "new-access-token"}}

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            client = CognitoClient("https://api.test.com")
            client.refresh_token("refresh-token-value", session_token="not-a-jwt")

        mock_idp.initiate_auth.assert_called_once_with(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": "refresh-token-value"},
            ClientId="test-client-id",
        )

    @responses.activate
    def test_cognito_config_is_cached(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "t", "RefreshToken": "r"}}

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            client = CognitoClient("https://api.test.com")
            client.authenticate("k", "s")
            client.authenticate("k", "s")

        # Only one HTTP call to cognito-config across two authenticate() invocations.
        assert len(responses.calls) == 1


class TestTokenAuthProvider:
    """Tests for TokenAuthProvider (production path: pre-supplied tokens)."""

    def test_get_session_token_returns_supplied_token(self):
        provider = TokenAuthProvider("https://api.test.com", "session-tok", "refresh-tok")
        assert provider.get_session_token() == "session-tok"

    @responses.activate
    def test_refresh_uses_refresh_token(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {"AuthenticationResult": {"AccessToken": "rotated-token"}}

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            provider = TokenAuthProvider("https://api.test.com", "old-session", "refresh-tok")
            new_token = provider.refresh()

        assert new_token == "rotated-token"
        assert provider.get_session_token() == "rotated-token"
        mock_idp.initiate_auth.assert_called_once()
        assert mock_idp.initiate_auth.call_args.kwargs["AuthFlow"] == "REFRESH_TOKEN_AUTH"

    def test_refresh_raises_when_no_refresh_token(self):
        provider = TokenAuthProvider("https://api.test.com", "session-tok", None)
        with pytest.raises(RuntimeError, match="no refresh token"):
            provider.refresh()


class TestKeySecretAuthProvider:
    """Tests for KeySecretAuthProvider (local development path: API key/secret)."""

    @responses.activate
    def test_authenticates_eagerly_on_construction(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.return_value = {
            "AuthenticationResult": {"AccessToken": "initial-tok", "RefreshToken": "initial-refresh"}
        }

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            provider = KeySecretAuthProvider("https://api.test.com", "api-key", "api-secret")

        assert provider.get_session_token() == "initial-tok"
        mock_idp.initiate_auth.assert_called_once_with(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "api-key", "PASSWORD": "api-secret"},
            ClientId="test-client-id",
        )

    @responses.activate
    def test_refresh_uses_refresh_token_when_available(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.side_effect = [
            {"AuthenticationResult": {"AccessToken": "tok-1", "RefreshToken": "refresh-1"}},
            {"AuthenticationResult": {"AccessToken": "tok-2"}},
        ]

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            provider = KeySecretAuthProvider("https://api.test.com", "api-key", "api-secret")
            new_token = provider.refresh()

        assert new_token == "tok-2"
        assert provider.get_session_token() == "tok-2"
        # Second call is the refresh flow
        assert mock_idp.initiate_auth.call_args_list[1].kwargs["AuthFlow"] == "REFRESH_TOKEN_AUTH"

    @responses.activate
    def test_refresh_falls_back_to_reauth_when_no_refresh_token(self):
        _add_cognito_config_response()

        mock_idp = Mock()
        mock_idp.initiate_auth.side_effect = [
            {"AuthenticationResult": {"AccessToken": "tok-1", "RefreshToken": None}},
            {"AuthenticationResult": {"AccessToken": "tok-2", "RefreshToken": "refresh-2"}},
        ]

        with patch("processor.clients.authentication_client.boto3.client", return_value=mock_idp):
            provider = KeySecretAuthProvider("https://api.test.com", "api-key", "api-secret")
            new_token = provider.refresh()

        assert new_token == "tok-2"
        # Fallback re-runs the USER_PASSWORD_AUTH flow.
        assert mock_idp.initiate_auth.call_args_list[1].kwargs["AuthFlow"] == "USER_PASSWORD_AUTH"
