"""Add EP completion inbox and ep_* handoff columns on activity_execution.

Revision ID: a1b2c3d4e5f6
Revises: 2c3d4e5f6071
Create Date: 2026-10-07

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "2c3d4e5f6071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create EP callback inbox and add handoff columns to activity_execution."""
    op.create_table(
        "execution_plane_completion_inbox",
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("work_item_id", sa.UUID(), nullable=False),
        sa.Column("state_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("result", JSONB(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint(
            "client_id",
            "work_item_id",
            "state_revision",
            name="uq_ep_completion_inbox_work_revision",
        ),
    )
    op.create_index(
        "ix_ep_completion_inbox_delivery",
        "execution_plane_completion_inbox",
        ["processed_at", "next_attempt_at", "lease_expires_at"],
    )

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


def downgrade() -> None:
    """Remove EP completion inbox and handoff columns from activity_execution."""
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

    op.drop_index("ix_ep_completion_inbox_delivery", table_name="execution_plane_completion_inbox")
    op.drop_table("execution_plane_completion_inbox")
