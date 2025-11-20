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

# Parser that transforms JSONValue into user's V type
# This is the main public API - users need this to provide custom parsers
ValueParser = Callable[[JSONValue], V]
