import functools
import logging

import requests

log = logging.getLogger()

# Default timeout for HTTP requests (connect, read) in seconds
DEFAULT_TIMEOUT = (10, 30)


class SessionManager:
    """Manages API session by delegating token retrieval and refresh to an AuthProvider."""

    def __init__(self, auth_provider, api_host: str, api_host2: str):
        """
        Args:
            auth_provider: AuthProvider instance (TokenAuthProvider or KeySecretAuthProvider)
            api_host: Primary Pennsieve API host
            api_host2: Secondary Pennsieve API host (packages / workflow services)
        """
        self._auth_provider = auth_provider
        self.api_host = api_host
        self.api_host2 = api_host2

    @property
    def session_token(self) -> str:
        return self._auth_provider.get_session_token()

    def refresh_session(self) -> None:
        self._auth_provider.refresh()


class BaseClient:
    """Base class for API clients with retry logic."""

    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager

    @staticmethod
    def retry_with_refresh(func):
        """Decorator that retries a request after refreshing the session on 401/403."""

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
