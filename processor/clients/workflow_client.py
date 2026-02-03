import logging

import requests
from clients.base_client import BaseClient, SessionManager

log = logging.getLogger()


class WorkflowClient(BaseClient):
    """Client for Pennsieve workflow management API."""

    def __init__(self, session_manager: SessionManager):
        """
        Initialize the workflow client.

        Args:
            session_manager: SessionManager instance for API access
        """
        super().__init__(session_manager)
        self.base_url = f"{session_manager.api_host}/workflows"

    @BaseClient.retry_with_refresh
    def get_integration(self, integration_id: str) -> dict:
        """
        Get details of a workflow integration.

        Args:
            integration_id: UUID of the workflow integration

        Returns:
            Integration details dict
        """
        url = f"{self.base_url}/instances/{integration_id}"

        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()

        return response.json()

    @BaseClient.retry_with_refresh
    def get_provenance_id(self, integration_id: str) -> str:
        """
        Get the provenance ID for a workflow integration.

        The provenance ID is used to link viewer assets back to their source.

        Args:
            integration_id: UUID of the workflow integration

        Returns:
            Provenance UUID string
        """
        integration = self.get_integration(integration_id)
        provenance_id = integration.get("provenanceId")

        if not provenance_id:
            raise ValueError(f"Integration {integration_id} has no provenanceId")

        log.info(f"Retrieved provenance ID: {provenance_id}")
        return provenance_id

    @BaseClient.retry_with_refresh
    def complete_integration(self, integration_id: str) -> None:
        """
        Mark a workflow integration as complete.

        Args:
            integration_id: UUID of the workflow integration
        """
        url = f"{self.base_url}/instances/{integration_id}/complete"

        response = requests.put(url, headers=self._get_headers())
        response.raise_for_status()

        log.info(f"Marked integration {integration_id} as complete")

    @BaseClient.retry_with_refresh
    def fail_integration(self, integration_id: str, error_message: str) -> None:
        """
        Mark a workflow integration as failed.

        Args:
            integration_id: UUID of the workflow integration
            error_message: Error message describing the failure
        """
        url = f"{self.base_url}/instances/{integration_id}/fail"
        payload = {"error": error_message}

        response = requests.put(url, json=payload, headers=self._get_headers())
        response.raise_for_status()

        log.info(f"Marked integration {integration_id} as failed: {error_message}")
