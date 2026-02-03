import json
import logging

import boto3
import requests
from clients.base_client import SessionManager

log = logging.getLogger()


class AuthenticationClient:
    """Handles AWS Cognito authentication for Pennsieve API."""

    def __init__(self, session_manager: SessionManager):
        """
        Initialize the authentication client.

        Args:
            session_manager: SessionManager instance
        """
        self.session_manager = session_manager
        # Register self with session manager for refresh capability
        session_manager.set_auth_client(self)

    def authenticate(self) -> str:
        """
        Authenticate with Pennsieve using API key/secret via AWS Cognito.

        Fetches Cognito configuration dynamically from the API, then
        authenticates using the provided credentials.

        Returns:
            Session token (access token)

        Raises:
            Exception: If authentication fails
        """
        log.info("Authenticating with Pennsieve...")

        try:
            # Fetch Cognito configuration from API
            url = f"{self.session_manager.api_host}/authentication/cognito-config"
            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.content)

            cognito_app_client_id = data["tokenPool"]["appClientId"]
            cognito_region = data["region"]

            # Create Cognito client and authenticate
            cognito_client = boto3.client(
                "cognito-idp",
                region_name=cognito_region,
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            login_response = cognito_client.initiate_auth(
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": self.session_manager.api_key,
                    "PASSWORD": self.session_manager.api_secret,
                },
                ClientId=cognito_app_client_id,
            )

            token = login_response["AuthenticationResult"]["AccessToken"]
            self.session_manager.session_token = token
            log.info("Authentication successful")
            return token

        except requests.HTTPError as e:
            log.error(f"Failed to reach authentication server: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode authentication response: {e}")
            raise
        except Exception as e:
            log.error(f"Failed to authenticate: {e}")
            raise
