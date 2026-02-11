"""Internal type definitions not exposed in public API."""

import hashlib
import time
from dataclasses import dataclass
from typing import Generic, NotRequired, Optional, TypedDict, TypeVar

# Re-export these from parent for internal use
from immukv.json_helpers import JSONValue
from immukv.types import Hash, KeyObjectETag, LogVersionId, Sequence, TimestampMs

K = TypeVar("K", bound=str)
V = TypeVar("V")


@dataclass
class RawEntry(Generic[K]):
    """Log entry with raw (undecoded) JSON value — for internal operations only."""

    key: K
    value: JSONValue
    timestamp_ms: TimestampMs[K]
    version_id: LogVersionId[K]
    sequence: Sequence[K]
    previous_version_id: Optional[LogVersionId[K]]
    hash: Hash[K]
    previous_hash: Hash[K]
    previous_key_object_etag: Optional[KeyObjectETag[K]] = None


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


class OrphanStatus(TypedDict, Generic[K], total=False):
    """Type definition for cached orphan status.

    Used to track whether the latest log entry is orphaned and cache
    the entry data for efficient retrieval without calling history().

    Parameterized by key type K. Stores RawEntry (undecoded value) to avoid
    invoking the value decoder on cross-type entries.
    """

    is_orphaned: bool  # True if latest entry is orphaned
    orphan_key: Optional[K]  # Key name of the orphaned entry (if orphaned)
    orphan_entry: Optional[RawEntry[K]]  # Raw entry data (if orphaned)
    checked_at: int  # Timestamp when this check was performed (client-level)


class LatestLogState(TypedDict, Generic[K], total=False):
    """Type definition for latest log state returned by _get_latest_and_repair.

    Contains information about the current log state and orphan repair results.
    """

    log_etag: Optional[str]  # ETag of current log (for optimistic locking), None for first entry
    prev_version_id: Optional[LogVersionId[K]]  # Previous log version ID
    prev_hash: Hash[K]  # Previous entry hash
    sequence: Sequence[K]  # Current sequence number
    can_write: Optional[bool]  # Whether client has write permission
    orphan_status: Optional[OrphanStatus[K]]  # Current orphan status


class LogEntryDict(TypedDict):
    """Log entry structure for JSON serialization.

    Fields with NotRequired[Optional[T]] support both semantics:
    - Before serialization: Field present with None value
    - After deserialization: Field might be missing (TypeScript undefined omits it)
    """

    sequence: int
    key: str
    value: JSONValue
    timestamp_ms: int
    hash: str
    previous_hash: str
    previous_version_id: NotRequired[Optional[str]]
    previous_key_object_etag: NotRequired[Optional[str]]


class KeyObjectDict(TypedDict):
    """Key object structure for JSON serialization.

    Key objects don't include previous_version_id or previous_key_object_etag.
    """

    key: str
    value: JSONValue
    timestamp_ms: int
    log_version_id: str
    sequence: int
    hash: str
    previous_hash: str


# Factory functions for branded types


def hash_compute(data: LogEntryForHash[K, V]) -> Hash[K]:
    """Compute SHA-256 hash from log entry data.

    Args:
        data: Log entry data to hash (excludes version_id, log_version_id, hash)

    Returns:
        Hash in format 'sha256:<64 hex characters>'
    """
    # Import here to avoid circular dependency
    from immukv._internal.json_helpers import dumps_canonical

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
