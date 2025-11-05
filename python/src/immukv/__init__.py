"""ImmuKV - Lightweight immutable key-value store using S3 versioning."""

from immukv.client import ImmuKVClient
from immukv.json_helpers import ValueParser
from immukv.types import Config, Entry, KeyNotFoundError

try:
    from importlib.metadata import version

    __version__ = version("immukv")
except Exception:
    __version__ = "unknown"

__all__ = [
    "ImmuKVClient",
    "ValueParser",
    "Config",
    "Entry",
    "KeyNotFoundError",
]
