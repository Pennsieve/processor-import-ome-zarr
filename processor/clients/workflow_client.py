import json
import logging
from dataclasses import dataclass

import requests

from .base_client import DEFAULT_TIMEOUT, BaseClient, SessionManager

log = logging.getLogger()


@dataclass(frozen=True, slots=True)
class WorkflowInstance:
    """Represents a workflow instance with its metadata."""

    id: str
    dataset_id: str
    package_ids: list[str]


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
    def get_workflow_instance(self, workflow_instance_id: str) -> WorkflowInstance:
        """
        Get details of a workflow instance.

        Args:
            workflow_instance_id: UUID of the workflow instance

        Returns:
            WorkflowInstance with id, dataset_id, and package_ids
        """
        url = f"{self.base_url}/instances/{workflow_instance_id}"

        try:
            response = requests.get(url, headers=self._get_headers(), timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            workflow_instance = WorkflowInstance(
                id=data["uuid"],
                dataset_id=data["datasetId"],
                package_ids=data["packageIds"],
            )

            return workflow_instance
        except requests.HTTPError as e:
            log.error(f"Failed to fetch workflow instance: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode workflow instance response: {e}")
            raise
