"""Type definitions for ImmuKV."""

import hashlib
import time
from dataclasses import dataclass
from typing import Generic, Optional, TypedDict, TypeVar

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


# Factory functions for branded types


def hash_compute(data: "LogEntryForHash[K, V]") -> Hash[K]:
    """Compute SHA-256 hash from log entry data.

    Args:
        data: Log entry data to hash (excludes version_id, log_version_id, hash)

    Returns:
        Hash in format 'sha256:<64 hex characters>'
    """
    # Import here to avoid circular dependency
    from immukv.json_helpers import dumps_canonical

    canonical_bytes = dumps_canonical(data)  # type: ignore[arg-type]
    hash_bytes = hashlib.sha256(canonical_bytes).digest()
    hash_hex = hash_bytes.hex()
    return Hash(f"sha256:{hash_hex}")


def hash_genesis() -> Hash[K]:
    """Return genesis hash for the first entry in a chain.

    Returns:
        Genesis hash 'sha256:genesis'
    """
    return Hash("sha256:genesis")


def hash_from_json(s: str) -> Hash[K]:
    """Parse hash from JSON string with validation.

    Args:
        s: Hash string from JSON

    Returns:
        Validated Hash type

    Raises:
        ValueError: If hash format is invalid
    """
    if not s.startswith("sha256:"):
        raise ValueError(f"Invalid hash format (must start with 'sha256:'): {s}")
    return Hash(s)


def sequence_initial() -> Sequence[K]:
    """Return initial sequence number before first entry.

    Returns:
        Sequence number -1 (will become 0 on first write)
    """
    return Sequence(-1)


def sequence_next(seq: Sequence[K]) -> Sequence[K]:
    """Increment sequence number.

    Args:
        seq: Current sequence number

    Returns:
        Next sequence number (seq + 1)
    """
    return Sequence(seq + 1)


def sequence_from_json(n: int) -> Sequence[K]:
    """Parse sequence from JSON with validation.

    Args:
        n: Sequence number from JSON

    Returns:
        Validated Sequence type

    Raises:
        ValueError: If sequence is invalid (< -1)
    """
    if n < -1:
        raise ValueError(f"Invalid sequence (must be >= -1): {n}")
    return Sequence(n)


def timestamp_now() -> TimestampMs[K]:
    """Return current timestamp in milliseconds.

    Returns:
        Current Unix epoch time in milliseconds
    """
    return TimestampMs(int(time.time() * 1000))


def timestamp_from_json(n: int) -> TimestampMs[K]:
    """Parse timestamp from JSON with validation.

    Args:
        n: Timestamp in milliseconds from JSON

    Returns:
        Validated TimestampMs type

    Raises:
        ValueError: If timestamp is invalid (<= 0)
    """
    if n <= 0:
        raise ValueError(f"Invalid timestamp (must be > 0): {n}")
    return TimestampMs(n)


@dataclass
class S3Credentials:
    """Explicit credentials for S3 authentication."""

    aws_access_key_id: str
    aws_secret_access_key: str


@dataclass
class S3Overrides:
    """Override default S3 client behavior (for MinIO in production, or testing with LocalStack/moto)."""

    # Custom S3 endpoint URL
    endpoint_url: Optional[str] = None

    # Explicit credentials (not needed for AWS with IAM roles)
    credentials: Optional[S3Credentials] = None

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


class LogEntryForHash(TypedDict, Generic[K, V]):
    """Type definition for log entry data used in hash calculation.

    This TypedDict specifies exactly which fields are included in the hash
    computation, making it impossible to accidentally include fields like
    'previous_version_id', 'log_version_id', or 'hash' itself.

    Parameterized by key type K and value type V for type safety.
    """

    sequence: Sequence[K]
    key: K
    value: V
    timestamp_ms: TimestampMs[K]
    previous_hash: Hash[K]


class OrphanStatus(TypedDict, Generic[K, V], total=False):
    """Type definition for cached orphan status.

    Used to track whether the latest log entry is orphaned and cache
    the entry data for efficient retrieval without calling history().

    Parameterized by key type K and value type V to match Entry type.
    """

    is_orphaned: bool  # True if latest entry is orphaned
    orphan_key: Optional[K]  # Key name of the orphaned entry (if orphaned)
    orphan_entry: Optional["Entry[K, V]"]  # Full entry data (if orphaned)
    checked_at: int  # Timestamp when this check was performed (client-level)


class LatestLogState(TypedDict, Generic[K, V], total=False):
    """Type definition for latest log state returned by _get_latest_and_repair.

    Contains information about the current log state and orphan repair results.
    """

    log_etag: Optional[str]  # ETag of current log (for optimistic locking), None for first entry
    prev_version_id: Optional[LogVersionId[K]]  # Previous log version ID
    prev_hash: Hash[K]  # Previous entry hash
    sequence: Sequence[K]  # Current sequence number
    can_write: Optional[bool]  # Whether client has write permission
    orphan_status: Optional[OrphanStatus[K, V]]  # Current orphan status


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
