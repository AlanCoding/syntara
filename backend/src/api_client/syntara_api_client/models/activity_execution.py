from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..models.activity_status import ActivityStatus
from ..models.node_type import NodeType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.activity_execution_input_data import ActivityExecutionInputData
    from ..models.activity_execution_labels import ActivityExecutionLabels
    from ..models.activity_execution_output_data_type_0 import ActivityExecutionOutputDataType0


T = TypeVar("T", bound="ActivityExecution")


@_attrs_define
class ActivityExecution:
    """Database model for activity executions.

    Persists activity execution state to enable querying after Temporal's retention
    period expires. Data is synced from Temporal events and cached in database.

    This model serves both as the database table and the API response schema.
    Activities are read-only from the API - they're created/updated by syncing from Temporal.

    Inherits from BaseResource:
        id: UUID primary key
        created_at: Creation timestamp
        updated_at: Last update timestamp
        labels: Optional key-value metadata

        Attributes:
            execution_id (UUID): Parent execution ID
            activity_name (str): Activity ID from workflow definition
            node_type (NodeType): Node types for V2 workflows (used by telemetry).
            temporal_activity_id (str): Temporal activity execution ID
            status (ActivityStatus): Activity execution status enumeration.
            id (UUID | Unset): Unique identifier for the resource Example: 550e8400-e29b-41d4-a716-446655440000.
            created_at (datetime.datetime | Unset): Timestamp when resource was created Example: 2025-10-09T12:00:00Z.
            updated_at (datetime.datetime | Unset): Timestamp when resource was last updated Example: 2025-10-09T12:30:00Z.
            labels (ActivityExecutionLabels | Unset): Key-value pairs for resource labeling and filtering Example:
                {'environment': 'production', 'region': 'us-east-1', 'team': 'platform'}.
            started_at (datetime.datetime | None | Unset): When activity started execution
            completed_at (datetime.datetime | None | Unset): When activity completed/failed
            input_data (ActivityExecutionInputData | Unset): Runtime input parameters
            output_data (ActivityExecutionOutputDataType0 | None | Unset): Activity results (if completed)
            error_details (None | str | Unset): Error information if failed
            retry_count (int | Unset): Number of retry attempts Default: 0.
            iteration (int | None | Unset): Iteration number if activity is within a loop (0-indexed)
            ep_task_token_ciphertext (None | str | Unset):
            ep_payload_ciphertext (None | str | Unset):
            ep_status (None | str | Unset):
            ep_activity_attempt (int | None | Unset):
            ep_last_status_check_at (datetime.datetime | None | Unset):
            ep_cancel_requested_at (datetime.datetime | None | Unset):
            ep_cancel_delivered_at (datetime.datetime | None | Unset):
            ep_cancel_next_attempt_at (datetime.datetime | None | Unset):
            ep_cancel_lease_expires_at (datetime.datetime | None | Unset):
            ep_cancel_attempts (int | Unset):  Default: 0.
            ep_cancel_last_error (None | str | Unset):
    """

    execution_id: UUID
    activity_name: str
    node_type: NodeType
    temporal_activity_id: str
    status: ActivityStatus
    id: UUID | Unset = UNSET
    created_at: datetime.datetime | Unset = UNSET
    updated_at: datetime.datetime | Unset = UNSET
    labels: ActivityExecutionLabels | Unset = UNSET
    started_at: datetime.datetime | None | Unset = UNSET
    completed_at: datetime.datetime | None | Unset = UNSET
    input_data: ActivityExecutionInputData | Unset = UNSET
    output_data: ActivityExecutionOutputDataType0 | None | Unset = UNSET
    error_details: None | str | Unset = UNSET
    retry_count: int | Unset = 0
    iteration: int | None | Unset = UNSET
    ep_task_token_ciphertext: None | str | Unset = UNSET
    ep_payload_ciphertext: None | str | Unset = UNSET
    ep_status: None | str | Unset = UNSET
    ep_activity_attempt: int | None | Unset = UNSET
    ep_last_status_check_at: datetime.datetime | None | Unset = UNSET
    ep_cancel_requested_at: datetime.datetime | None | Unset = UNSET
    ep_cancel_delivered_at: datetime.datetime | None | Unset = UNSET
    ep_cancel_next_attempt_at: datetime.datetime | None | Unset = UNSET
    ep_cancel_lease_expires_at: datetime.datetime | None | Unset = UNSET
    ep_cancel_attempts: int | Unset = 0
    ep_cancel_last_error: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.activity_execution_output_data_type_0 import ActivityExecutionOutputDataType0

        execution_id = str(self.execution_id)

        activity_name = self.activity_name

        node_type = self.node_type.value

        temporal_activity_id = self.temporal_activity_id

        status = self.status.value

        id: str | Unset = UNSET
        if not isinstance(self.id, Unset):
            id = str(self.id)

        created_at: str | Unset = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        updated_at: str | Unset = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        labels: dict[str, Any] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = self.labels.to_dict()

        started_at: None | str | Unset
        if isinstance(self.started_at, Unset):
            started_at = UNSET
        elif isinstance(self.started_at, datetime.datetime):
            started_at = self.started_at.isoformat()
        else:
            started_at = self.started_at

        completed_at: None | str | Unset
        if isinstance(self.completed_at, Unset):
            completed_at = UNSET
        elif isinstance(self.completed_at, datetime.datetime):
            completed_at = self.completed_at.isoformat()
        else:
            completed_at = self.completed_at

        input_data: dict[str, Any] | Unset = UNSET
        if not isinstance(self.input_data, Unset):
            input_data = self.input_data.to_dict()

        output_data: dict[str, Any] | None | Unset
        if isinstance(self.output_data, Unset):
            output_data = UNSET
        elif isinstance(self.output_data, ActivityExecutionOutputDataType0):
            output_data = self.output_data.to_dict()
        else:
            output_data = self.output_data

        error_details: None | str | Unset
        if isinstance(self.error_details, Unset):
            error_details = UNSET
        else:
            error_details = self.error_details

        retry_count = self.retry_count

        iteration: int | None | Unset
        if isinstance(self.iteration, Unset):
            iteration = UNSET
        else:
            iteration = self.iteration

        ep_task_token_ciphertext: None | str | Unset
        if isinstance(self.ep_task_token_ciphertext, Unset):
            ep_task_token_ciphertext = UNSET
        else:
            ep_task_token_ciphertext = self.ep_task_token_ciphertext

        ep_payload_ciphertext: None | str | Unset
        if isinstance(self.ep_payload_ciphertext, Unset):
            ep_payload_ciphertext = UNSET
        else:
            ep_payload_ciphertext = self.ep_payload_ciphertext

        ep_status: None | str | Unset
        if isinstance(self.ep_status, Unset):
            ep_status = UNSET
        else:
            ep_status = self.ep_status

        ep_activity_attempt: int | None | Unset
        if isinstance(self.ep_activity_attempt, Unset):
            ep_activity_attempt = UNSET
        else:
            ep_activity_attempt = self.ep_activity_attempt

        ep_last_status_check_at: None | str | Unset
        if isinstance(self.ep_last_status_check_at, Unset):
            ep_last_status_check_at = UNSET
        elif isinstance(self.ep_last_status_check_at, datetime.datetime):
            ep_last_status_check_at = self.ep_last_status_check_at.isoformat()
        else:
            ep_last_status_check_at = self.ep_last_status_check_at

        ep_cancel_requested_at: None | str | Unset
        if isinstance(self.ep_cancel_requested_at, Unset):
            ep_cancel_requested_at = UNSET
        elif isinstance(self.ep_cancel_requested_at, datetime.datetime):
            ep_cancel_requested_at = self.ep_cancel_requested_at.isoformat()
        else:
            ep_cancel_requested_at = self.ep_cancel_requested_at

        ep_cancel_delivered_at: None | str | Unset
        if isinstance(self.ep_cancel_delivered_at, Unset):
            ep_cancel_delivered_at = UNSET
        elif isinstance(self.ep_cancel_delivered_at, datetime.datetime):
            ep_cancel_delivered_at = self.ep_cancel_delivered_at.isoformat()
        else:
            ep_cancel_delivered_at = self.ep_cancel_delivered_at

        ep_cancel_next_attempt_at: None | str | Unset
        if isinstance(self.ep_cancel_next_attempt_at, Unset):
            ep_cancel_next_attempt_at = UNSET
        elif isinstance(self.ep_cancel_next_attempt_at, datetime.datetime):
            ep_cancel_next_attempt_at = self.ep_cancel_next_attempt_at.isoformat()
        else:
            ep_cancel_next_attempt_at = self.ep_cancel_next_attempt_at

        ep_cancel_lease_expires_at: None | str | Unset
        if isinstance(self.ep_cancel_lease_expires_at, Unset):
            ep_cancel_lease_expires_at = UNSET
        elif isinstance(self.ep_cancel_lease_expires_at, datetime.datetime):
            ep_cancel_lease_expires_at = self.ep_cancel_lease_expires_at.isoformat()
        else:
            ep_cancel_lease_expires_at = self.ep_cancel_lease_expires_at

        ep_cancel_attempts = self.ep_cancel_attempts

        ep_cancel_last_error: None | str | Unset
        if isinstance(self.ep_cancel_last_error, Unset):
            ep_cancel_last_error = UNSET
        else:
            ep_cancel_last_error = self.ep_cancel_last_error

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "execution_id": execution_id,
                "activity_name": activity_name,
                "node_type": node_type,
                "temporal_activity_id": temporal_activity_id,
                "status": status,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at
        if labels is not UNSET:
            field_dict["labels"] = labels
        if started_at is not UNSET:
            field_dict["started_at"] = started_at
        if completed_at is not UNSET:
            field_dict["completed_at"] = completed_at
        if input_data is not UNSET:
            field_dict["input_data"] = input_data
        if output_data is not UNSET:
            field_dict["output_data"] = output_data
        if error_details is not UNSET:
            field_dict["error_details"] = error_details
        if retry_count is not UNSET:
            field_dict["retry_count"] = retry_count
        if iteration is not UNSET:
            field_dict["iteration"] = iteration
        if ep_task_token_ciphertext is not UNSET:
            field_dict["ep_task_token_ciphertext"] = ep_task_token_ciphertext
        if ep_payload_ciphertext is not UNSET:
            field_dict["ep_payload_ciphertext"] = ep_payload_ciphertext
        if ep_status is not UNSET:
            field_dict["ep_status"] = ep_status
        if ep_activity_attempt is not UNSET:
            field_dict["ep_activity_attempt"] = ep_activity_attempt
        if ep_last_status_check_at is not UNSET:
            field_dict["ep_last_status_check_at"] = ep_last_status_check_at
        if ep_cancel_requested_at is not UNSET:
            field_dict["ep_cancel_requested_at"] = ep_cancel_requested_at
        if ep_cancel_delivered_at is not UNSET:
            field_dict["ep_cancel_delivered_at"] = ep_cancel_delivered_at
        if ep_cancel_next_attempt_at is not UNSET:
            field_dict["ep_cancel_next_attempt_at"] = ep_cancel_next_attempt_at
        if ep_cancel_lease_expires_at is not UNSET:
            field_dict["ep_cancel_lease_expires_at"] = ep_cancel_lease_expires_at
        if ep_cancel_attempts is not UNSET:
            field_dict["ep_cancel_attempts"] = ep_cancel_attempts
        if ep_cancel_last_error is not UNSET:
            field_dict["ep_cancel_last_error"] = ep_cancel_last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.activity_execution_input_data import ActivityExecutionInputData
        from ..models.activity_execution_labels import ActivityExecutionLabels
        from ..models.activity_execution_output_data_type_0 import ActivityExecutionOutputDataType0

        d = dict(src_dict)
        execution_id = UUID(d.pop("execution_id"))

        activity_name = d.pop("activity_name")

        node_type = NodeType(d.pop("node_type"))

        temporal_activity_id = d.pop("temporal_activity_id")

        status = ActivityStatus(d.pop("status"))

        _id = d.pop("id", UNSET)
        id: UUID | Unset
        if isinstance(_id, Unset):
            id = UNSET
        else:
            id = UUID(_id)

        _created_at = d.pop("created_at", UNSET)
        created_at: datetime.datetime | Unset
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = isoparse(_created_at)

        _updated_at = d.pop("updated_at", UNSET)
        updated_at: datetime.datetime | Unset
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = isoparse(_updated_at)

        _labels = d.pop("labels", UNSET)
        labels: ActivityExecutionLabels | Unset
        if isinstance(_labels, Unset):
            labels = UNSET
        else:
            labels = ActivityExecutionLabels.from_dict(_labels)

        def _parse_started_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                started_at_type_0 = isoparse(data)

                return started_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        started_at = _parse_started_at(d.pop("started_at", UNSET))

        def _parse_completed_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                completed_at_type_0 = isoparse(data)

                return completed_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        completed_at = _parse_completed_at(d.pop("completed_at", UNSET))

        _input_data = d.pop("input_data", UNSET)
        input_data: ActivityExecutionInputData | Unset
        if isinstance(_input_data, Unset):
            input_data = UNSET
        else:
            input_data = ActivityExecutionInputData.from_dict(_input_data)

        def _parse_output_data(data: object) -> ActivityExecutionOutputDataType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                output_data_type_0 = ActivityExecutionOutputDataType0.from_dict(data)

                return output_data_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ActivityExecutionOutputDataType0 | None | Unset, data)

        output_data = _parse_output_data(d.pop("output_data", UNSET))

        def _parse_error_details(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        error_details = _parse_error_details(d.pop("error_details", UNSET))

        retry_count = d.pop("retry_count", UNSET)

        def _parse_iteration(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        iteration = _parse_iteration(d.pop("iteration", UNSET))

        def _parse_ep_task_token_ciphertext(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        ep_task_token_ciphertext = _parse_ep_task_token_ciphertext(d.pop("ep_task_token_ciphertext", UNSET))

        def _parse_ep_payload_ciphertext(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        ep_payload_ciphertext = _parse_ep_payload_ciphertext(d.pop("ep_payload_ciphertext", UNSET))

        def _parse_ep_status(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        ep_status = _parse_ep_status(d.pop("ep_status", UNSET))

        def _parse_ep_activity_attempt(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        ep_activity_attempt = _parse_ep_activity_attempt(d.pop("ep_activity_attempt", UNSET))

        def _parse_ep_last_status_check_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ep_last_status_check_at_type_0 = isoparse(data)

                return ep_last_status_check_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        ep_last_status_check_at = _parse_ep_last_status_check_at(d.pop("ep_last_status_check_at", UNSET))

        def _parse_ep_cancel_requested_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ep_cancel_requested_at_type_0 = isoparse(data)

                return ep_cancel_requested_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        ep_cancel_requested_at = _parse_ep_cancel_requested_at(d.pop("ep_cancel_requested_at", UNSET))

        def _parse_ep_cancel_delivered_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ep_cancel_delivered_at_type_0 = isoparse(data)

                return ep_cancel_delivered_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        ep_cancel_delivered_at = _parse_ep_cancel_delivered_at(d.pop("ep_cancel_delivered_at", UNSET))

        def _parse_ep_cancel_next_attempt_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ep_cancel_next_attempt_at_type_0 = isoparse(data)

                return ep_cancel_next_attempt_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        ep_cancel_next_attempt_at = _parse_ep_cancel_next_attempt_at(d.pop("ep_cancel_next_attempt_at", UNSET))

        def _parse_ep_cancel_lease_expires_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ep_cancel_lease_expires_at_type_0 = isoparse(data)

                return ep_cancel_lease_expires_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        ep_cancel_lease_expires_at = _parse_ep_cancel_lease_expires_at(d.pop("ep_cancel_lease_expires_at", UNSET))

        ep_cancel_attempts = d.pop("ep_cancel_attempts", UNSET)

        def _parse_ep_cancel_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        ep_cancel_last_error = _parse_ep_cancel_last_error(d.pop("ep_cancel_last_error", UNSET))

        activity_execution = cls(
            execution_id=execution_id,
            activity_name=activity_name,
            node_type=node_type,
            temporal_activity_id=temporal_activity_id,
            status=status,
            id=id,
            created_at=created_at,
            updated_at=updated_at,
            labels=labels,
            started_at=started_at,
            completed_at=completed_at,
            input_data=input_data,
            output_data=output_data,
            error_details=error_details,
            retry_count=retry_count,
            iteration=iteration,
            ep_task_token_ciphertext=ep_task_token_ciphertext,
            ep_payload_ciphertext=ep_payload_ciphertext,
            ep_status=ep_status,
            ep_activity_attempt=ep_activity_attempt,
            ep_last_status_check_at=ep_last_status_check_at,
            ep_cancel_requested_at=ep_cancel_requested_at,
            ep_cancel_delivered_at=ep_cancel_delivered_at,
            ep_cancel_next_attempt_at=ep_cancel_next_attempt_at,
            ep_cancel_lease_expires_at=ep_cancel_lease_expires_at,
            ep_cancel_attempts=ep_cancel_attempts,
            ep_cancel_last_error=ep_cancel_last_error,
        )

        return activity_execution
