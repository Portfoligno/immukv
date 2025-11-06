"""ImmuKV - Lightweight immutable key-value store using S3 versioning."""

from immukv.client import ImmuKVClient
from immukv.json_helpers import ValueParser
from immukv.types import (
    Config,
    Entry,
    KeyNotFoundError,
    hash_compute,
    hash_from_json,
    hash_genesis,
    sequence_from_json,
    sequence_initial,
    sequence_next,
    timestamp_from_json,
    timestamp_now,
)

__version__ = "__VERSION_EeEyfbyVyf4JmFfk__"

__all__ = [
    "ImmuKVClient",
    "ValueParser",
    "Config",
    "Entry",
    "KeyNotFoundError",
    "hash_compute",
    "hash_from_json",
    "hash_genesis",
    "sequence_from_json",
    "sequence_initial",
    "sequence_next",
    "timestamp_from_json",
    "timestamp_now",
]
