"""Typed wrapper around boto3 S3 client to satisfy strict mypy checking.

This wrapper centralizes all type casts for boto3 S3 API responses,
allowing the main client code to work with Any-free types.
"""

import json
from typing import Any, Dict, List, Optional, TypedDict, TypeVar, Union, cast

# Import K and V type variables to match client
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


# Response type definitions (only fields we actually use, no Any types)


class S3GetObjectResponse(TypedDict):
    """S3 GetObject response with only used fields."""

    Body: object  # StreamingBody - opaque, we only call .read()
    ETag: str
    VersionId: str


class S3PutObjectResponse(TypedDict):
    """S3 PutObject response with only used fields."""

    ETag: str
    VersionId: str


class S3HeadObjectResponse(TypedDict):
    """S3 HeadObject response with only used fields."""

    ETag: str
    VersionId: str


class S3ObjectVersion(TypedDict):
    """S3 object version in list response."""

    Key: str
    VersionId: str
    IsLatest: bool
    ETag: str


class S3ListObjectVersionsPage(TypedDict, total=False):
    """S3 ListObjectVersions response page."""

    Versions: List[S3ObjectVersion]
    IsTruncated: bool
    NextKeyMarker: str
    NextVersionIdMarker: str


class S3Object(TypedDict):
    """S3 object in list response."""

    Key: str


class S3ListObjectsV2Page(TypedDict, total=False):
    """S3 ListObjectsV2 response page."""

    Contents: List[S3Object]
    IsTruncated: bool
    NextContinuationToken: str


class ErrorResponse(TypedDict):
    """Boto3 error response structure."""

    Code: str
    Message: str


class ClientErrorResponse(TypedDict):
    """Boto3 ClientError response structure."""

    Error: ErrorResponse


class TypedS3Client:
    """Type-safe wrapper around boto3 S3 client.

    Centralizes all casts from boto3's Any-containing types to our
    clean Any-free type definitions. This allows strict mypy checking
    (disallow_any_expr) while working with boto3.
    """

    def __init__(self, s3_client: Any) -> None:  # type: ignore[misc,explicit-any]
        """Initialize with a boto3 S3 client."""
        self._s3 = s3_client  # type: ignore[misc]

    def get_object(
        self,
        Bucket: str,
        Key: str,
        VersionId: Optional[str] = None,
    ) -> S3GetObjectResponse:
        """Get object from S3."""
        kwargs: Dict[str, Any] = {"Bucket": Bucket, "Key": Key}  # type: ignore[misc,explicit-any]
        if VersionId:
            kwargs["VersionId"] = VersionId  # type: ignore[misc]
        return cast(S3GetObjectResponse, self._s3.get_object(**kwargs))  # type: ignore[misc]

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: Optional[str] = None,
        IfMatch: Optional[str] = None,
        IfNoneMatch: Optional[str] = None,
        ServerSideEncryption: Optional[str] = None,
        SSEKMSKeyId: Optional[str] = None,
    ) -> S3PutObjectResponse:
        """Put object to S3."""
        kwargs: Dict[str, Any] = {  # type: ignore[misc,explicit-any]
            "Bucket": Bucket,
            "Key": Key,
            "Body": Body,
        }
        if ContentType:
            kwargs["ContentType"] = ContentType  # type: ignore[misc]
        if IfMatch:
            kwargs["IfMatch"] = IfMatch  # type: ignore[misc]
        if IfNoneMatch:
            kwargs["IfNoneMatch"] = IfNoneMatch  # type: ignore[misc]
        if ServerSideEncryption:
            kwargs["ServerSideEncryption"] = ServerSideEncryption  # type: ignore[misc]
        if SSEKMSKeyId:
            kwargs["SSEKMSKeyId"] = SSEKMSKeyId  # type: ignore[misc]
        return cast(S3PutObjectResponse, self._s3.put_object(**kwargs))  # type: ignore[misc]

    def head_object(
        self,
        Bucket: str,
        Key: str,
    ) -> S3HeadObjectResponse:
        """Get object metadata from S3."""
        return cast(S3HeadObjectResponse, self._s3.head_object(Bucket=Bucket, Key=Key))  # type: ignore[misc]

    def list_object_versions(
        self,
        Bucket: str,
        Prefix: str,
        KeyMarker: Optional[str] = None,
        VersionIdMarker: Optional[str] = None,
    ) -> S3ListObjectVersionsPage:
        """List object versions."""
        kwargs: Dict[str, Any] = {"Bucket": Bucket, "Prefix": Prefix}  # type: ignore[misc,explicit-any]
        if KeyMarker:
            kwargs["KeyMarker"] = KeyMarker  # type: ignore[misc]
        if VersionIdMarker:
            kwargs["VersionIdMarker"] = VersionIdMarker  # type: ignore[misc]
        return cast(S3ListObjectVersionsPage, self._s3.list_object_versions(**kwargs))  # type: ignore[misc]

    def get_paginator(self, operation_name: str) -> Any:  # type: ignore[misc,explicit-any]
        """Get paginator for list operations."""
        return self._s3.get_paginator(operation_name)  # type: ignore[misc]


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
