from unittest.mock import Mock

import pytest
import requests

from processor.clients.base_client import BaseClient, SessionManager


class TestSessionManager:
    """Tests for SessionManager class."""

    def test_initialization(self):
        """Should initialize with provided auth provider and host config."""
        auth_provider = Mock()
        manager = SessionManager(
            auth_provider,
            api_host="https://api.example.com",
            api_host2="https://api2.example.com",
        )

        assert manager.api_host == "https://api.example.com"
        assert manager.api_host2 == "https://api2.example.com"

    def test_session_token_delegates_to_auth_provider(self):
        """Should retrieve current session token from the auth provider."""
        auth_provider = Mock()
        auth_provider.get_session_token.return_value = "current-token"
        manager = SessionManager(auth_provider, "", "")

        assert manager.session_token == "current-token"
        auth_provider.get_session_token.assert_called_once()

    def test_refresh_session_delegates_to_auth_provider(self):
        """Should call refresh on the auth provider."""
        auth_provider = Mock()
        manager = SessionManager(auth_provider, "", "")

        manager.refresh_session()

        auth_provider.refresh.assert_called_once()


class TestBaseClient:
    """Tests for BaseClient class."""

    def test_get_headers(self, mock_session_manager):
        """Should return headers with bearer token."""
        client = BaseClient(mock_session_manager)
        headers = client._get_headers()

        assert headers["Authorization"] == "Bearer mock-token-12345"
        assert headers["Content-Type"] == "application/json"

    def test_retry_with_refresh_success(self, mock_session_manager):
        """Should not retry on successful request."""

        class TestClient(BaseClient):
            @BaseClient.retry_with_refresh
            def test_method(self):
                return "success"

        client = TestClient(mock_session_manager)
        result = client.test_method()

        assert result == "success"
        mock_session_manager.refresh_session.assert_not_called()

    def test_retry_with_refresh_on_401(self, mock_session_manager):
        """Should retry after refreshing session on 401 error."""
        call_count = 0

        class TestClient(BaseClient):
            @BaseClient.retry_with_refresh
            def test_method(self):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    response = Mock()
                    response.status_code = 401
                    raise requests.HTTPError(response=response)
                return "success"

        client = TestClient(mock_session_manager)
        result = client.test_method()

        assert result == "success"
        assert call_count == 2
        mock_session_manager.refresh_session.assert_called_once()

    def test_retry_with_refresh_on_403(self, mock_session_manager):
        """Should retry after refreshing session on 403 error."""
        call_count = 0

        class TestClient(BaseClient):
            @BaseClient.retry_with_refresh
            def test_method(self):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    response = Mock()
                    response.status_code = 403
                    raise requests.HTTPError(response=response)
                return "success"

        client = TestClient(mock_session_manager)
        result = client.test_method()

        assert result == "success"
        mock_session_manager.refresh_session.assert_called_once()

    def test_retry_with_refresh_raises_other_errors(self, mock_session_manager):
        """Should raise non-401/403 HTTP errors without retry."""

        class TestClient(BaseClient):
            @BaseClient.retry_with_refresh
            def test_method(self):
                response = Mock()
                response.status_code = 500
                raise requests.HTTPError(response=response)

        client = TestClient(mock_session_manager)

        with pytest.raises(requests.HTTPError):
            client.test_method()

        mock_session_manager.refresh_session.assert_not_called()
