"""Syntara AO database registration for dev-environment clusters.

This module deliberately imports from the ``syntara`` application package.
It is dev tooling only — not part of the ``execution_plane`` package API —
and represents a deliberate boundary crossing between the two codebases.

The interface here is intentionally thin and will be refactored into a
proper store/registry class once the execution-plane and syntara cluster
models are further consolidated.
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession as SQLModelAsyncSession

from syntara.authz.models import Project
from syntara.core.services.secret_service import create_secret_service
from syntara.credentials.models.credential import Credential
from syntara.credentials.models.credential_type import CredentialType
from syntara.integrations.models.integration import (
    Integration,
    IntegrationScope,
    IntegrationStatus,
    IntegrationType,
)
from syntara.integrations.models.integration_configuration import OpenShiftConfiguration


async def register_integration_record(
    *,
    name: str,
    endpoint: str,
    namespace: str,
    api_key: str,
    actor_id: uuid.UUID,
    database_url: str,
) -> None:
    """Create or refresh the Credential and Integration for a dev environment.

    Upserts an HTTP Bearer Token Credential (storing ``api_key`` as the token)
    and a global OpenShift Integration linked to that credential. Re-running
    updates the stored token and endpoint without creating duplicates.

    Raises:
        RuntimeError: If required seed data (credential type or default project)
            is absent from the database.
    """
    engine = create_async_engine(database_url)
    try:
        session_maker = async_sessionmaker(engine, class_=SQLModelAsyncSession, expire_on_commit=False)
        async with session_maker() as session:
            bearer_type = (
                await session.exec(select(CredentialType).where(CredentialType.name == "HTTP Bearer Token"))
            ).first()
            if bearer_type is None:
                raise RuntimeError("'HTTP Bearer Token' credential type not found; run db-seed first")
            default_project = (await session.exec(select(Project).where(col(Project.is_default).is_(True)))).first()
            if default_project is None:
                raise RuntimeError("Default project not found; run db-seed first")

            secret_service = create_secret_service(session)
            credential_name = f"{name}-openshift-token"
            existing_credential = (
                await session.exec(
                    select(Credential).where(
                        Credential.name == credential_name,
                        Credential.project_id == default_project.id,
                    )
                )
            ).first()

            if existing_credential is not None:
                if existing_credential.secret_id is not None:
                    await secret_service.update_secret(existing_credential.secret_id, {"token": api_key})
                credential_id = existing_credential.id
            else:
                secret_id = await secret_service.create_secret({"token": api_key})
                credential = Credential(
                    name=credential_name,
                    credential_type_id=bearer_type.id,
                    secret_id=secret_id,
                    project_id=default_project.id,
                    enabled=True,
                    created_by=actor_id,
                    updated_by=actor_id,
                    labels={},
                )
                session.add(credential)
                await session.flush()
                credential_id = credential.id

            configuration = OpenShiftConfiguration(base_url=endpoint, namespace=namespace)
            integration_name = f"{name}-openshift"
            existing_integration = (
                await session.exec(select(Integration).where(Integration.name == integration_name))
            ).first()

            if existing_integration is not None:
                existing_integration.configuration = configuration
                existing_integration.management_credential_id = credential_id
                existing_integration.updated_by = actor_id
                session.add(existing_integration)
            else:
                integration = Integration(
                    name=integration_name,
                    integration_type=IntegrationType.OPENSHIFT,
                    management_credential_id=credential_id,
                    configuration=configuration,
                    scope=IntegrationScope.GLOBAL,
                    validation_status=IntegrationStatus.UNKNOWN,
                    enabled=True,
                    created_by=actor_id,
                    updated_by=actor_id,
                    labels={},
                )
                session.add(integration)

            await session.commit()
    finally:
        await engine.dispose()
