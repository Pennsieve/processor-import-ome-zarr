import responses

from processor.clients.workflow_client import WorkflowClient, WorkflowInstance


class TestWorkflowInstance:
    """Tests for WorkflowInstance class."""

    def test_initialization(self):
        """Should store provided values."""
        instance = WorkflowInstance(
            id="instance-123",
            dataset_id="dataset-456",
            package_ids=["pkg-1", "pkg-2"],
        )

        assert instance.id == "instance-123"
        assert instance.dataset_id == "dataset-456"
        assert instance.package_ids == ["pkg-1", "pkg-2"]


class TestWorkflowClient:
    """Tests for WorkflowClient class."""

    def test_initialization(self, mock_session_manager):
        """Should initialize with correct base URL."""
        client = WorkflowClient(mock_session_manager)
        assert client.base_url == "https://api.pennsieve.net/workflows"

    @responses.activate
    def test_get_workflow_instance(self, mock_session_manager):
        """Should get workflow instance details."""
        responses.add(
            responses.GET,
            "https://api.pennsieve.net/workflows/instances/instance-123",
            json={
                "uuid": "instance-123",
                "datasetId": "dataset-456",
                "packageIds": ["pkg-1", "pkg-2"],
            },
            status=200,
        )

        client = WorkflowClient(mock_session_manager)
        result = client.get_workflow_instance("instance-123")

        assert isinstance(result, WorkflowInstance)
        assert result.id == "instance-123"
        assert result.dataset_id == "dataset-456"
        assert result.package_ids == ["pkg-1", "pkg-2"]
