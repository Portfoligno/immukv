"""Helper functions for S3 operations.

These functions are not part of the public API and should only be used internally.
"""

import json
from typing import Any, Dict, Union, List, cast

from immukv._internal.s3_types import ClientErrorResponse

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


def read_body_as_json(body: object) -> Dict[str, JSONValue]:
    """Read S3 Body object and parse as JSON.

    Centralizes json.loads() cast to satisfy disallow_any_expr.
    """
    body_data = cast(Any, body).read()  # type: ignore[misc,explicit-any]
    json_str = cast(str, body_data.decode("utf-8") if isinstance(body_data, bytes) else body_data)  # type: ignore[misc]
    return cast(Dict[str, JSONValue], json.loads(json_str))  # type: ignore[misc,explicit-any]


def get_error_code(error: Exception) -> str:
    """Extract error code from ClientError.

    Centralizes ClientError response access to satisfy disallow_any_expr.
    """
    error_response = cast(ClientErrorResponse, cast(Any, error).response)  # type: ignore[misc,explicit-any]
    return cast(str, error_response["Error"]["Code"])
