"""Internal JSON helper functions not exposed in public API."""

import json
from typing import Callable, Dict, Optional, TypeVar, cast

from immukv._internal.types import RawEntry, hash_from_json, sequence_from_json, timestamp_from_json
from immukv.json_helpers import JSONValue
from immukv.types import Entry, KeyObjectETag, LogVersionId

# Type variables for generic key and value types
K = TypeVar("K", bound=str)
V = TypeVar("V")


def strip_none_values(data: Dict[str, JSONValue]) -> Dict[str, JSONValue]:
    """Strip None values from the immediate outer layer of a dictionary.

    This ensures consistency with TypeScript behavior where undefined values
    are omitted from JSON serialization. Only strips None from the top level,
    does not recurse into nested dictionaries.

    Args:
        data: Dictionary potentially containing None values

    Returns:
        New dictionary with None values removed from top level
    """
    return {k: v for k, v in data.items() if v is not None}


def get_str(data: Dict[str, JSONValue], key: str) -> str:
    """Extract string field from parsed JSON dict.

    Validates that the field is actually a string at runtime.

    Raises:
        TypeError: If field is not a string
        KeyError: If field is missing
    """
    value = data[key]
    if not isinstance(value, str):
        raise TypeError(f"Expected string for field '{key}', got {type(value).__name__}: {value!r}")
    return value


def get_int(data: Dict[str, JSONValue], key: str) -> int:
    """Extract int field from parsed JSON dict.

    Validates that the field is actually an int at runtime.
    Note: bool is a subclass of int in Python, so we explicitly reject booleans.

    Raises:
        TypeError: If field is not an int or is a bool
        KeyError: If field is missing
    """
    value = data[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Expected int for field '{key}', got {type(value).__name__}: {value!r}")
    return value


def get_optional_str(data: Dict[str, JSONValue], key: str) -> Optional[str]:
    """Extract optional string field from parsed JSON dict."""
    value = data.get(key)
    return cast(Optional[str], value)


def get_optional_int(data: Dict[str, JSONValue], key: str) -> Optional[int]:
    """Extract optional int field from parsed JSON dict."""
    value = data.get(key)
    return cast(Optional[int], value)


def entry_from_key_object(
    data: Dict[str, JSONValue], value_decoder: Callable[[JSONValue], V]
) -> Entry[K, V]:
    """Construct Entry from key object JSON data.

    Key objects store: key, value, timestamp_ms, log_version_id, sequence, hash, previous_hash.
    They do NOT store: previous_version_id, previous_key_object_etag.

    Args:
        data: Parsed JSON dict from S3 key object
        value_decoder: Decoder to transform JSONValue to user's V type
    """
    # Decode value using user's decoder
    value = value_decoder(data["value"])

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
    data: Dict[str, JSONValue], version_id: LogVersionId[K], value_decoder: Callable[[JSONValue], V]
) -> Entry[K, V]:
    """Construct Entry from log JSON data with explicit version_id.

    Log entries store all fields including previous_version_id and previous_key_object_etag.
    The version_id parameter is the S3 version ID of the log entry itself.

    Args:
        data: Parsed JSON dict from S3 log entry
        version_id: S3 version ID of the log entry
        value_decoder: Decoder to transform JSONValue to user's V type
    """
    prev_version_id_str = get_optional_str(data, "previous_version_id")
    prev_key_etag_str = get_optional_str(data, "previous_key_object_etag")

    # Decode value using user's decoder
    value = value_decoder(data["value"])

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


def raw_entry_from_log(data: Dict[str, JSONValue], version_id: LogVersionId[K]) -> RawEntry[K]:
    """Construct RawEntry from log JSON data with explicit version_id.

    Identical to entry_from_log but without the value_decoder parameter —
    keeps the value as raw JSONValue for internal operations.

    Args:
        data: Parsed JSON dict from S3 log entry
        version_id: S3 version ID of the log entry
    """
    prev_version_id_str = get_optional_str(data, "previous_version_id")
    prev_key_etag_str = get_optional_str(data, "previous_key_object_etag")

    return RawEntry(
        key=cast(K, get_str(data, "key")),
        value=data["value"],
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
