"""ImmuKV - Lightweight immutable key-value store using S3 versioning."""

from immukv.client import ImmuKVClient
from immukv.json_helpers import JSONValue, ValueParser
from immukv.types import Config, Entry, KeyNotFoundError, ReadOnlyError

__version__ = "__VERSION_EeEyfbyVyf4JmFfk__"

__all__ = [
    "ImmuKVClient",
    "ValueParser",
    "JSONValue",
    "Config",
    "Entry",
    "KeyNotFoundError",
    "ReadOnlyError",
]
