"""Type definitions for ImmuKV."""

from dataclasses import dataclass
from typing import Any, Generic, Optional, TypedDict, TypeVar

# Type variables for generic key and value types
K = TypeVar("K", bound=str)  # Key type must be a subtype of str
V = TypeVar("V")  # Value type can be anything


@dataclass
class Config:
    """Client configuration."""

    # S3 configuration (all mandatory)
    s3_bucket: str
    s3_region: str
    s3_prefix: str

    # Optional: encryption
    kms_key_id: Optional[str] = None

    # Optional: orphan repair policy
    repair_check_interval_ms: int = 300000  # 5 minutes (in-memory tracking)

    # Optional: read-only mode (disables all repair attempts)
    read_only: bool = False  # If True, never attempt to write key objects


class LogEntryForHash(TypedDict, Generic[K, V]):
    """Type definition for log entry data used in hash calculation.

    This TypedDict specifies exactly which fields are included in the hash
    computation, making it impossible to accidentally include fields like
    'previous_version_id', 'log_version_id', or 'hash' itself.

    Parameterized by key type K and value type V for type safety.
    """

    sequence: int
    key: K
    value: V
    timestamp_ms: int
    previous_hash: str


class OrphanStatus(TypedDict, total=False):
    """Type definition for cached orphan status.

    Used to track whether the latest log entry is orphaned and cache
    the entry data for efficient retrieval without calling history().
    """

    is_orphaned: bool  # True if latest entry is orphaned
    orphan_key: Optional[str]  # Key name of the orphaned entry (if orphaned)
    orphan_entry: Optional["Entry[Any, Any]"]  # Full entry data (if orphaned)
    checked_at: int  # Timestamp when this check was performed (client-level)


@dataclass
class Entry(Generic[K, V]):
    """Represents a log entry."""

    key: K
    value: V
    timestamp_ms: int  # Unix epoch milliseconds
    version_id: str  # Log version ID for this entry
    sequence: int  # Client-maintained counter
    previous_version_id: Optional[str]
    hash: str
    previous_hash: str
    previous_key_object_etag: Optional[str] = None  # Previous key object ETag at log write time


class KeyNotFoundError(Exception):
    """Raised when a key is not found and no orphan fallback is available."""

    pass


class ReadOnlyError(Exception):
    """Raised when attempting to write in read-only mode."""

    pass
