"""Integration tests for OpenShift cluster sync on Integration CRUD."""

from uuid import UUID

import pytest
from execution_plane.models.cluster import Cluster
from httpx import AsyncClient
from sqlalchemy import select
from sqlmodel import col
from sqlmodel.ext.asyncio.session import AsyncSession

from syntara.integrations.models.integration import IntegrationType

BASE_URL = "/api/v1/integrations"


class TestOpenShiftClusterSync:
    """Verify cluster records are created and deleted with OpenShift integrations."""

    @pytest.mark.asyncio
    async def test_create_openshift_integration_creates_cluster(
        self,
        auth_client: AsyncClient,
        db: AsyncSession,
        http_bearer_token_credential_id: UUID,
    ) -> None:
        """Creating an OpenShift integration should create a cluster record."""
        payload = {
            "name": "test-cluster-create",
            "integration_type": IntegrationType.OPENSHIFT.value,
            "configuration": {
                "integration_type": IntegrationType.OPENSHIFT.value,
                "base_url": "https://api.example.com:6443",
                "namespace": "default",
                "insecure_skip_tls_verify": False,
                "allow_http": False,
                "ca_certificate": None,
            },
            "management_credential_id": str(http_bearer_token_credential_id),
            "scope": "global",
        }

        resp = await auth_client.post(BASE_URL, json=payload)
        assert resp.status_code == 201
        integration_id = resp.json()["id"]

        # Verify cluster was created with matching name
        result = await db.execute(select(Cluster).where(col(Cluster.name) == "test-cluster-create"))
        cluster = result.scalar_one_or_none()
        assert cluster is not None
        assert cluster.endpoint == "https://api.example.com:6443"
        # Labels should contain integration_id
        assert cluster.labels.get("integration_id") == integration_id
        assert cluster.labels.get("integration_name") == "test-cluster-create"

    @pytest.mark.asyncio
    async def test_delete_openshift_integration_deletes_cluster(
        self,
        auth_client: AsyncClient,
        db: AsyncSession,
        http_bearer_token_credential_id: UUID,
    ) -> None:
        """Deleting an OpenShift integration should delete the cluster record."""
        # Create integration
        payload = {
            "name": "test-cluster-delete",
            "integration_type": IntegrationType.OPENSHIFT.value,
            "configuration": {
                "integration_type": IntegrationType.OPENSHIFT.value,
                "base_url": "https://api.example.com:6443",
                "namespace": "default",
                "insecure_skip_tls_verify": False,
                "allow_http": False,
                "ca_certificate": None,
            },
            "management_credential_id": str(http_bearer_token_credential_id),
            "scope": "global",
        }

        resp = await auth_client.post(BASE_URL, json=payload)
        assert resp.status_code == 201
        integration_id = resp.json()["id"]

        # Verify cluster was created
        result = await db.execute(select(Cluster).where(col(Cluster.name) == "test-cluster-delete"))
        cluster = result.scalar_one_or_none()
        assert cluster is not None
        cluster_id = cluster.id

        # Delete integration
        delete_resp = await auth_client.delete(f"{BASE_URL}/{integration_id}")
        assert delete_resp.status_code == 204

        # Verify cluster was deleted (or marked DRAINING)
        result = await db.execute(select(Cluster).where(col(Cluster.id) == cluster_id))
        cluster = result.scalar_one_or_none()
        # Cluster should be marked DRAINING (not fully deleted yet, but marked for deletion)
        assert cluster is not None
        assert cluster.status.value == "draining"
