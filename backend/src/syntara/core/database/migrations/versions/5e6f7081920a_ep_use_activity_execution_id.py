"""Replace EP binding table with columns on activity_execution.

Revision ID: 5e6f7081920a
Revises: 3d4e5f607182
Create Date: 2026-10-07

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5e6f7081920a"
down_revision: str | Sequence[str] | None = "3d4e5f607182"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Add EP handoff columns to activity_execution
    op.add_column("activity_execution", sa.Column("ep_task_token_ciphertext", sa.Text(), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_payload_ciphertext", sa.Text(), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_status", sa.String(length=32), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_activity_attempt", sa.Integer(), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_last_status_check_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_cancel_requested_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("activity_execution", sa.Column("ep_cancel_delivered_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "activity_execution", sa.Column("ep_cancel_next_attempt_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "activity_execution", sa.Column("ep_cancel_lease_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "activity_execution",
        sa.Column("ep_cancel_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column("activity_execution", sa.Column("ep_cancel_last_error", sa.String(length=1000), nullable=True))
    op.create_index("ix_activity_execution_ep_status", "activity_execution", ["ep_status"])
    op.create_index(
        "ix_activity_execution_ep_cancel",
        "activity_execution",
        ["ep_cancel_delivered_at", "ep_cancel_next_attempt_at", "ep_cancel_lease_expires_at"],
    )

    # Drop old EP binding table (indexes first)
    op.drop_index("ix_ep_activity_bindings_cancel_delivery", table_name="execution_plane_activity_bindings")
    op.drop_index("ix_ep_activity_bindings_status_updated", table_name="execution_plane_activity_bindings")
    op.drop_table("execution_plane_activity_bindings")

    # Update completion inbox: drop request_id, update unique constraint
    op.drop_constraint("uq_ep_completion_inbox_request_revision", "execution_plane_completion_inbox", type_="unique")
    op.drop_column("execution_plane_completion_inbox", "request_id")
    op.create_unique_constraint(
        "uq_ep_completion_inbox_work_revision",
        "execution_plane_completion_inbox",
        ["client_id", "project_id", "work_item_id", "state_revision"],
    )


def downgrade() -> None:
    # Revert completion inbox changes
    op.drop_constraint("uq_ep_completion_inbox_work_revision", "execution_plane_completion_inbox", type_="unique")
    op.add_column(
        "execution_plane_completion_inbox",
        sa.Column("request_id", sa.String(length=64), nullable=False, server_default=""),
    )
    op.create_unique_constraint(
        "uq_ep_completion_inbox_request_revision",
        "execution_plane_completion_inbox",
        ["client_id", "project_id", "request_id", "state_revision"],
    )

    # Recreate EP binding table
    op.create_table(
        "execution_plane_activity_bindings",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=False),
        sa.Column("temporal_workflow_id", sa.String(length=255), nullable=False),
        sa.Column("temporal_run_id", sa.String(length=255), nullable=False),
        sa.Column("temporal_activity_id", sa.String(length=255), nullable=False),
        sa.Column("activity_attempt", sa.Integer(), nullable=False),
        sa.Column("task_token_ciphertext", sa.Text(), nullable=False),
        sa.Column("request_payload_ciphertext", sa.Text(), nullable=False),
        sa.Column("work_item_id", sa.UUID(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_status_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cancel_last_error", sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_id", name="uq_ep_activity_bindings_request_id"),
    )
    op.create_index(
        "ix_ep_activity_bindings_status_updated", "execution_plane_activity_bindings", ["status", "updated_at"]
    )
    op.create_index(
        "ix_ep_activity_bindings_cancel_delivery",
        "execution_plane_activity_bindings",
        ["cancel_delivered_at", "cancel_next_attempt_at", "cancel_lease_expires_at"],
    )

    # Remove EP columns from activity_execution
    op.drop_index("ix_activity_execution_ep_cancel", table_name="activity_execution")
    op.drop_index("ix_activity_execution_ep_status", table_name="activity_execution")
    op.drop_column("activity_execution", "ep_cancel_last_error")
    op.drop_column("activity_execution", "ep_cancel_attempts")
    op.drop_column("activity_execution", "ep_cancel_lease_expires_at")
    op.drop_column("activity_execution", "ep_cancel_next_attempt_at")
    op.drop_column("activity_execution", "ep_cancel_delivered_at")
    op.drop_column("activity_execution", "ep_cancel_requested_at")
    op.drop_column("activity_execution", "ep_last_status_check_at")
    op.drop_column("activity_execution", "ep_activity_attempt")
    op.drop_column("activity_execution", "ep_status")
    op.drop_column("activity_execution", "ep_payload_ciphertext")
    op.drop_column("activity_execution", "ep_task_token_ciphertext")
