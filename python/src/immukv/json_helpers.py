"""Helper functions for JSON parsing and Entry construction.

Centralizes JSON field extraction with proper typing to satisfy disallow_any_expr.
"""

import json
from typing import Callable, Dict, List, Optional, TypeVar, Union, cast

from immukv.types import (
    Entry,
    KeyObjectETag,
    LogVersionId,
    hash_from_json,
    sequence_from_json,
    timestamp_from_json,
)

# Type variables for generic key and value types
K = TypeVar("K", bound=str)
V = TypeVar("V")

# Represents any valid JSON value
JSONValue = Union[
    None,
    bool,
    int,
    float,
    str,
    List["JSONValue"],
    Dict[str, "JSONValue"],
]

# Parser that transforms JSONValue into user's V type
ValueParser = Callable[[JSONValue], V]


def get_str(data: Dict[str, JSONValue], key: str) -> str:
    """Extract string field from parsed JSON dict."""
    return cast(str, data[key])


def get_int(data: Dict[str, JSONValue], key: str) -> int:
    """Extract int field from parsed JSON dict."""
    return cast(int, data[key])


def get_optional_str(data: Dict[str, JSONValue], key: str) -> Optional[str]:
    """Extract optional string field from parsed JSON dict."""
    value = data.get(key)
    return cast(Optional[str], value)


def get_optional_int(data: Dict[str, JSONValue], key: str) -> Optional[int]:
    """Extract optional int field from parsed JSON dict."""
    value = data.get(key)
    return cast(Optional[int], value)


def entry_from_key_object(data: Dict[str, JSONValue], value_parser: ValueParser[V]) -> Entry[K, V]:
    """Construct Entry from key object JSON data.

    Key objects store: key, value, timestamp_ms, log_version_id, sequence, hash, previous_hash.
    They do NOT store: previous_version_id, previous_key_object_etag.

    Args:
        data: Parsed JSON dict from S3 key object
        value_parser: Parser to transform JSONValue to user's V type
    """
    # Parse value using user's parser
    value = value_parser(data["value"])

    return Entry(
        key=cast(K, get_str(data, "key")),
        value=value,
        timestamp_ms=timestamp_from_json(get_int(data, "timestamp_ms")),
        version_id=LogVersionId(get_str(data, "log_version_id")),
        sequence=sequence_from_json(get_int(data, "sequence")),
        previous_version_id=None,
        hash=hash_from_json(get_str(data, "hash")),
        previous_hash=hash_from_json(get_str(data, "previous_hash")),
        previous_key_object_etag=None,
    )


def entry_from_log(
    data: Dict[str, JSONValue], version_id: LogVersionId[K], value_parser: ValueParser[V]
) -> Entry[K, V]:
    """Construct Entry from log JSON data with explicit version_id.

    Log entries store all fields including previous_version_id and previous_key_object_etag.
    The version_id parameter is the S3 version ID of the log entry itself.

    Args:
        data: Parsed JSON dict from S3 log entry
        version_id: S3 version ID of the log entry
        value_parser: Parser to transform JSONValue to user's V type
    """
    prev_version_id_str = get_optional_str(data, "previous_version_id")
    prev_key_etag_str = get_optional_str(data, "previous_key_object_etag")

    # Parse value using user's parser
    value = value_parser(data["value"])

    return Entry(
        key=cast(K, get_str(data, "key")),
        value=value,
        timestamp_ms=timestamp_from_json(get_int(data, "timestamp_ms")),
        version_id=version_id,
        sequence=sequence_from_json(get_int(data, "sequence")),
        previous_version_id=(
            LogVersionId(prev_version_id_str) if prev_version_id_str is not None else None
        ),
        hash=hash_from_json(get_str(data, "hash")),
        previous_hash=hash_from_json(get_str(data, "previous_hash")),
        previous_key_object_etag=(
            KeyObjectETag(prev_key_etag_str) if prev_key_etag_str is not None else None
        ),
    )


def dumps_canonical(data: JSONValue) -> bytes:
    """Serialize data to canonical JSON format for S3 storage.

    Uses sorted keys and minimal separators for deterministic serialization.
    This ensures consistent ETags for idempotent repair operations.

    Uses ensure_ascii=True (default) to avoid Unicode normalization issues
    and ensure deterministic output across all platforms and languages.

    Returns UTF-8 encoded bytes ready for S3 upload.
    """
    json_str: str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return json_str.encode("utf-8")
