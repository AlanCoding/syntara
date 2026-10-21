"""Execution Plane read proxy following EP's pagination/filtering wire contract.

Query controls mirror syntara.core.models.base.query_params.BaseListParams.
Documented filters correspond to execution_plane.models.work_item.WorkItem and
execution_plane.models.execution_target.ExecutionTarget field allowlists; dynamic
bracket filters are forwarded to EP's execution_plane.query_filters parser.

Project/RBAC semantics are a separate followup. This transport alignment retains
the existing facade authorization checks and adds no project associations.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 — Pydantic resolves model annotations at runtime
from typing import Annotated, Any, Literal
from uuid import UUID  # noqa: TC003 — Pydantic resolves model annotations at runtime

import structlog
from fastapi import Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, ValidationError

from syntara.authz.dependencies import ProjectScopeFilter
from syntara.authz.engine import AllowedProjectsResult  # noqa: TC001 — FastAPI resolves dependency annotations
from syntara.core.models.base import BaseListParams
from syntara.core.syntara_router import SyntaraRouter
from syntara.execution_plane.client import (
    ExecutionPlaneHttpClient,
    ExecutionPlaneRejectedError,
    ExecutionPlaneUnavailableError,
)

logger = structlog.stdlib.get_logger(__name__)

router = SyntaraRouter(prefix="/api/execution_plane/v1", tags=["Execution Plane"])

_target_scope = ProjectScopeFilter("execution_target", "read")
_work_item_scope = ProjectScopeFilter("work_item", "read")


class ExecutionTargetListParams(BaseListParams):
    """Document execution_plane.models.execution_target.ExecutionTarget filters once per field.

    As with UserListParams, bracket operators stay dynamic and EP validates values.
    String annotations allow comma-separated equality/membership values on the wire.
    """

    id: str | None = Field(default=None, description="Target UUID; id[in]=uuid1,uuid2 for membership")
    cluster_id: str | None = Field(default=None, description="Physical cluster UUID; supports eq and in")
    name: str | None = Field(default=None, description="Name; supports eq, in, contains, and starts_with")
    endpoint: str | None = Field(default=None, description="Endpoint; supports eq, in, contains, and starts_with")
    status: str | None = Field(default=None, description="Target status; supports eq and in")
    backend_type: str | None = Field(default=None, description="Backend type; supports eq and in")
    enabled: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    is_default: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    created_at: str | None = Field(
        default=None, description="Creation time; supports eq, in, gt, gte, lt, lte, and isnull"
    )
    last_ran_at: str | None = Field(default=None, description="Last run time; supports ranges and isnull")
    status_message: str | None = Field(default=None, description="Status message; supports text operators and isnull")


class WorkItemListParams(BaseListParams):
    """Document execution_plane.models.work_item.WorkItem filters once per field."""

    id: str | None = Field(default=None, description="Work UUID; id[in]=uuid1,uuid2 for membership")
    status: str | None = Field(default=None, description="Work status; supports eq and in")
    created_at: str | None = Field(
        default=None, description="Creation time; supports eq, in, gt, gte, lt, lte, and isnull"
    )
    claimed_at: str | None = Field(default=None, description="Claim time; supports ranges and isnull")
    completed_at: str | None = Field(default=None, description="Completion time; supports ranges and isnull")
    resource_cleanup_status: str | None = Field(default=None, description="Cleanup status; supports eq and in")
    resource_cleanup_error: str | None = Field(
        default=None, description="Cleanup error; supports text operators and isnull"
    )


class ExecutionTargetListResponse(BaseModel):
    """Paginated list response for ExecutionTarget."""

    resources: list[ExecutionTargetFacadeRead]
    next: str | None = None
    prev: str | None = None
    total: int | None = None


class WorkItemListResponse(BaseModel):
    """Paginated list response for WorkItem."""

    resources: list[WorkItemFacadeRead]
    next: str | None = None
    prev: str | None = None
    total: int | None = None


class KubernetesPlacement(BaseModel):
    """Kubernetes scheduling metadata returned by the Execution Plane."""

    type: Literal["kubernetes"]
    namespace: str = Field(min_length=1, max_length=63, pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    node_selectors: list[str] = Field(default_factory=list)
    tolerations: list[str] = Field(default_factory=list)


class RHELPlacement(BaseModel):
    """RHEL placement marker returned by the Execution Plane."""

    type: Literal["rhel"]


class ExecutionTargetFacadeRead(BaseModel):
    """Safe target fields available to AO's authorized management API."""

    id: UUID
    cluster_id: UUID
    name: str
    backend_type: str
    endpoint: str
    placement: Annotated[KubernetesPlacement | RHELPlacement, Field(discriminator="type")]
    status: str
    enabled: bool
    is_default: bool
    status_message: str | None = None
    labels: dict[str, str]
    created_at: datetime
    last_ran_at: datetime | None = None


class WorkItemFacadeRead(BaseModel):
    """Safe work state; internal tokens and storage metadata stay private."""

    id: UUID
    status: str
    result: dict[str, object] | None = None
    created_at: datetime
    claimed_at: datetime | None = None
    completed_at: datetime | None = None


@router.get(
    "/execution_targets",
    operation_id="list_execution_targets",
    summary="List execution targets",
    description=(
        "Retrieve registered execution targets. Filters use field=value or field[operator]=value; "
        "labels[key]=value matches a label and an empty value tests key existence. "
        "EP validates fields and operators. Keep filters, sort, and limit unchanged when following opaque cursors."
    ),
    responses={
        502: {"description": "Invalid Execution Plane response"},
        503: {"description": "Execution Plane unavailable"},
    },
)
async def list_execution_targets(
    request: Request,
    allowed_projects: Annotated[AllowedProjectsResult, Depends(_target_scope)],
    params: Annotated[ExecutionTargetListParams, Query()],
) -> ExecutionTargetListResponse:
    """Read registered execution targets from EP after AO permission checks."""
    try:
        async with ExecutionPlaneHttpClient() as client:
            page = await _list_ep_resources(
                client,
                operation="execution_targets",
                allowed_projects=allowed_projects,
                params=params,
                query_params=dict(request.query_params),
            )
        resources = [ExecutionTargetFacadeRead.model_validate(item) for item in page["resources"]]
    except ExecutionPlaneUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Execution Plane unavailable",
        ) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Invalid Execution Plane response") from exc
    logger.info("Listed execution targets", count=len(resources))
    return ExecutionTargetListResponse(
        resources=resources,
        next=page.get("next"),
        prev=page.get("prev"),
        total=page.get("total"),
    )


@router.get(
    "/work_items",
    operation_id="list_work_items",
    summary="List work items",
    description=(
        "Retrieve work items. Filters use field=value or field[operator]=value; comma-separated "
        "values within one field/operator use OR and different fields/operators use AND. "
        "EP validates fields and operators. Keep filters, sort, and limit unchanged when following opaque cursors."
    ),
    responses={
        502: {"description": "Invalid Execution Plane response"},
        503: {"description": "Execution Plane unavailable"},
    },
)
async def list_work_items(
    request: Request,
    allowed_projects: Annotated[AllowedProjectsResult, Depends(_work_item_scope)],
    params: Annotated[WorkItemListParams, Query()],
) -> WorkItemListResponse:
    """Read work-item state from EP after AO permission checks."""
    try:
        async with ExecutionPlaneHttpClient() as client:
            page = await _list_ep_resources(
                client,
                operation="work_items",
                allowed_projects=allowed_projects,
                params=params,
                query_params=dict(request.query_params),
            )
        resources = [WorkItemFacadeRead.model_validate(item) for item in page["resources"]]
    except ExecutionPlaneUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Execution Plane unavailable",
        ) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Invalid Execution Plane response") from exc
    logger.info("Listed work items", count=len(resources))
    return WorkItemListResponse(
        resources=resources,
        next=page.get("next"),
        prev=page.get("prev"),
        total=page.get("total"),
    )


async def _list_ep_resources(
    client: ExecutionPlaneHttpClient,
    *,
    operation: Literal["execution_targets", "work_items"],
    allowed_projects: AllowedProjectsResult,
    params: BaseListParams,
    query_params: dict[str, str],
) -> dict[str, Any]:
    """List EP resources after AO permission checks. EP has no project scope."""
    if not allowed_projects.all_projects and not allowed_projects.project_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No project-scoped access to EP resources")
    filters = {key: value for key, value in query_params.items() if key not in BaseListParams.model_fields}
    try:
        list_page = client.list_work_items if operation == "work_items" else client.list_execution_targets
        return await list_page(
            limit=params.limit,
            cursor=params.cursor,
            sort=params.sort,
            include_total=params.include_total,
            filters=filters,
        )
    except ExecutionPlaneRejectedError as exc:
        if exc.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Invalid Execution Plane response") from exc
