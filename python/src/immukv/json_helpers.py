"""Public JSON type definitions for ImmuKV.

Only exports types that users need for custom value parsing.
Internal helper functions are in immukv._internal.json_helpers.
"""

from typing import Callable, Dict, List, TypeVar, Union

V = TypeVar("V")

# Represents any valid JSON value
# This is public API - users need it for type annotations
JSONValue = Union[
    None,
    bool,
    int,
    float,
    str,
    List["JSONValue"],
    Dict[str, "JSONValue"],
]

# Decoder that transforms JSONValue into user's V type
# Users provide this to parse JSON from S3 into their custom types
ValueDecoder = Callable[[JSONValue], V]

# Encoder that transforms user's V type into JSONValue
# Users provide this to serialize their custom types to JSON for S3
ValueEncoder = Callable[[V], JSONValue]
