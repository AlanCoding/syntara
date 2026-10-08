"""AO-owned persistence for EP callback events."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class ExecutionPlaneCompletionInbox(SQLModel, table=True):
    """AO's durable, deduplicated inbox for authenticated EP result events."""

    __tablename__ = "execution_plane_completion_inbox"
    __table_args__ = (
        UniqueConstraint(
            "client_id",
            "project_id",
            "work_item_id",
            "state_revision",
            name="uq_ep_completion_inbox_work_revision",
        ),
        Index("ix_ep_completion_inbox_delivery", "processed_at", "next_attempt_at", "lease_expires_at"),
    )

    event_id: uuid.UUID = Field(primary_key=True)
    client_id: str = Field(sa_column=Column(String(128), nullable=False))
    project_id: uuid.UUID
    work_item_id: uuid.UUID
    state_revision: int = Field(sa_column=Column(Integer, nullable=False))
    status: str = Field(sa_column=Column(String(32), nullable=False))
    result: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    completed_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    received_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    processed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    attempts: int = Field(default=0, sa_column=Column(Integer, nullable=False, server_default="0"))
    next_attempt_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    lease_expires_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    last_error: str | None = Field(default=None, sa_column=Column(String(1000), nullable=True))
