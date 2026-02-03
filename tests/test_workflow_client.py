import pytest
import responses
from clients.workflow_client import WorkflowClient


class TestWorkflowClient:
    """Tests for WorkflowClient class."""

    def test_initialization(self, mock_session_manager):
        """Should initialize with correct base URL."""
        client = WorkflowClient(mock_session_manager)
        assert client.base_url == "https://api.pennsieve.net/workflows"

    @responses.activate
    def test_get_integration(self, mock_session_manager):
        """Should get integration details."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/workflows/instances/integration-123",
            json={
                "id": "integration-123",
                "provenanceId": "provenance-456",
                "status": "running",
            },
            status=200,
        )

        client = WorkflowClient(mock_session_manager)
        result = client.get_integration("integration-123")

        assert result["id"] == "integration-123"
        assert result["provenanceId"] == "provenance-456"

    @responses.activate
    def test_get_provenance_id(self, mock_session_manager):
        """Should extract provenance ID from integration."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/workflows/instances/integration-123",
            json={
                "id": "integration-123",
                "provenanceId": "provenance-456",
            },
            status=200,
        )

        client = WorkflowClient(mock_session_manager)
        result = client.get_provenance_id("integration-123")

        assert result == "provenance-456"

    @responses.activate
    def test_get_provenance_id_missing(self, mock_session_manager):
        """Should raise error when provenance ID is missing."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/workflows/instances/integration-123",
            json={
                "id": "integration-123",
            },
            status=200,
        )

        client = WorkflowClient(mock_session_manager)

        with pytest.raises(ValueError, match="has no provenanceId"):
            client.get_provenance_id("integration-123")

    @responses.activate
    def test_complete_integration(self, mock_session_manager):
        """Should mark integration as complete."""
        responses.add(
            responses.PUT,
            "https://api.pennsieve.net/workflows/instances/integration-123/complete",
            status=200,
        )

        client = WorkflowClient(mock_session_manager)
        client.complete_integration("integration-123")

        assert len(responses.calls) == 1

    @responses.activate
    def test_fail_integration(self, mock_session_manager):
        """Should mark integration as failed with error message."""
        responses.add(
            responses.PUT,
            "https://api.pennsieve.net/workflows/instances/integration-123/fail",
            status=200,
        )

        client = WorkflowClient(mock_session_manager)
        client.fail_integration("integration-123", "Something went wrong")

        assert len(responses.calls) == 1
        import json

        body = json.loads(responses.calls[0].request.body)
        assert body["error"] == "Something went wrong"
