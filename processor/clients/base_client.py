import functools
import logging

import requests

log = logging.getLogger()

# Default timeout for HTTP requests (connect, read) in seconds
DEFAULT_TIMEOUT = (10, 30)


class SessionManager:
    """Manages API session and authentication tokens."""

    def __init__(self, api_host: str, api_host2: str, api_key: str, api_secret: str):
        """
        Initialize the session manager.

        Args:
            api_host: Primary Pennsieve API host
            api_host2: Secondary Pennsieve API host (import service)
            api_key: Pennsieve API key
            api_secret: Pennsieve API secret
        """
        self.api_host = api_host
        self.api_host2 = api_host2
        self.api_key = api_key
        self.api_secret = api_secret
        self.session_token = None
        self._auth_client = None

    def set_auth_client(self, auth_client):
        """Set the authentication client for session refresh."""
        self._auth_client = auth_client

    def refresh_session(self):
        """Refresh the session token."""
        if self._auth_client is None:
            raise RuntimeError("Authentication client not set")
        self.session_token = self._auth_client.authenticate()
        log.info("Session token refreshed")


class BaseClient:
    """Base class for API clients with retry logic."""

    def __init__(self, session_manager: SessionManager):
        """
        Initialize the base client.

        Args:
            session_manager: SessionManager instance for API access
        """
        self.session_manager = session_manager

    @staticmethod
    def retry_with_refresh(func):
        """
        Decorator that retries a request after refreshing the session on 401/403.

        Args:
            func: Function to wrap

        Returns:
            Wrapped function with retry logic
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except requests.HTTPError as e:
                if e.response.status_code in (401, 403):
                    log.warning("Received 401/403, refreshing session and retrying")
                    self.session_manager.refresh_session()
                    return func(self, *args, **kwargs)
                raise

        return wrapper

    def _get_headers(self) -> dict:
        """Get standard headers for API requests."""
        return {
            "Authorization": f"Bearer {self.session_manager.session_token}",
            "Content-Type": "application/json",
        }
