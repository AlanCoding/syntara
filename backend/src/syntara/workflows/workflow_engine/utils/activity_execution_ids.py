"""Activity UUIDs shared by the history monitor and EP dispatch."""

from uuid import UUID, uuid5


def activity_execution_id(execution_id: UUID, temporal_activity_id: str) -> UUID:
    """Derive the same identity independently, without a database lookup."""
    return uuid5(execution_id, temporal_activity_id)
