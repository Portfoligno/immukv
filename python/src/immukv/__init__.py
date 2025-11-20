"""ImmuKV - Lightweight immutable key-value store using S3 versioning."""

from immukv.client import ImmuKVClient
from immukv.json_helpers import JSONValue, ValueParser
from immukv.types import (
    Config,
    Entry,
    Hash,
    KeyNotFoundError,
    KeyObjectETag,
    KeyVersionId,
    LogVersionId,
    ReadOnlyError,
    S3Credentials,
    S3Overrides,
    Sequence,
    TimestampMs,
)

__version__ = "__VERSION_EeEyfbyVyf4JmFfk__"

__all__ = [
    # Client
    "ImmuKVClient",
    # JSON types
    "JSONValue",
    "ValueParser",
    # Core types
    "Config",
    "Entry",
    # Config helper types
    "S3Credentials",
    "S3Overrides",
    # Branded types (Entry and method parameter field types)
    "LogVersionId",
    "KeyVersionId",
    "KeyObjectETag",
    "Hash",
    "Sequence",
    "TimestampMs",
    # Exceptions
    "KeyNotFoundError",
    "ReadOnlyError",
]
