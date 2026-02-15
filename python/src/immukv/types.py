"""Type definitions for ImmuKV."""

from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Generic, Optional, TypeVar, Union

# Type variables for generic key and value types
K = TypeVar("K", bound=str)  # Key type must be a subtype of str
V = TypeVar("V")  # Value type can be anything


# Nominal types parameterized by key type
# These are nominal types to prevent mixing version IDs from different contexts
class LogVersionId(str, Generic[K]):
    """Version ID for an entry in the global log for key K."""

    pass


class KeyVersionId(str, Generic[K]):
    """Version ID for the key object file keys/{K}.json."""

    pass


class KeyObjectETag(str, Generic[K]):
    """ETag for a key object file keys/{K}.json (for optimistic locking).

    Format: '"<md5_hex>"' (quoted MD5 hex string)
    Stored in log entry to enable idempotent repair without refetch.
    Used with IfMatch for update, IfNoneMatch='*' for create.
    """

    pass


class Hash(str, Generic[K]):
    """SHA-256 hash for an entry associated with key K.

    Format: 'sha256:<64 hex characters>'
    Forms a chain: each entry's hash includes the previous entry's hash.
    """

    pass


class Sequence(int, Generic[K]):
    """Sequence number for an entry associated with key K.

    Client-maintained counter that increments with each write.
    """

    pass


class TimestampMs(int, Generic[K]):
    """Unix epoch timestamp in milliseconds for an entry associated with key K."""

    pass


@dataclass
class S3Credentials:
    """Explicit credentials for S3 authentication."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    expires_at: Optional[datetime] = None


CredentialProvider = Callable[[], Awaitable[S3Credentials]]


@dataclass
class S3Overrides:
    """Override default S3 client behavior (for MinIO in production, or testing with LocalStack/moto)."""

    # Custom S3 endpoint URL
    endpoint_url: Optional[str] = None

    # Explicit credentials (not needed for AWS with IAM roles).
    # Can be a static S3Credentials object or an async callable that returns one.
    credentials: Union[S3Credentials, CredentialProvider, None] = None

    # Use path-style URLs instead of virtual-hosted style (required for MinIO)
    force_path_style: bool = False


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

    # Optional: override default S3 client behavior
    overrides: Optional[S3Overrides] = None


@dataclass
class Entry(Generic[K, V]):
    """Represents a log entry."""

    key: K
    value: V
    timestamp_ms: TimestampMs[K]  # Unix epoch milliseconds
    version_id: LogVersionId[K]  # Log version ID for this entry
    sequence: Sequence[K]  # Client-maintained counter
    previous_version_id: Optional[LogVersionId[K]]
    hash: Hash[K]
    previous_hash: Hash[K]
    previous_key_object_etag: Optional[KeyObjectETag[K]] = (
        None  # Previous key object ETag at log write time
    )


class KeyNotFoundError(Exception):
    """Raised when a key is not found and no orphan fallback is available."""

    pass


class ReadOnlyError(Exception):
    """Raised when attempting to write in read-only mode."""

    pass
