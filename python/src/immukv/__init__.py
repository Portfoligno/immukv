"""ImmuKV - Lightweight immutable key-value store using S3 versioning."""

from immukv.client import ImmuKVClient
from immukv.types import Config, Entry, KeyNotFoundError

__version__ = "0.1.0-dev"

__all__ = [
    "ImmuKVClient",
    "Config",
    "Entry",
    "KeyNotFoundError",
]
