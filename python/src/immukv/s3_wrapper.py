"""Typed wrapper around boto3 S3 client to satisfy strict mypy checking.

This wrapper centralizes all type casts for boto3 S3 API responses,
allowing the main client code to work with Any-free types.
"""

import json
from typing import Any, Dict, List, Literal, Optional, TypedDict, TypeVar, Union, cast

from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.paginator import ListObjectsV2Paginator
from mypy_boto3_s3.type_defs import (
    GetObjectRequestTypeDef,
    ListObjectVersionsRequestTypeDef,
    PutObjectRequestTypeDef,
)

from immukv.types import KeyObjectETag, KeyVersionId, LogVersionId

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

    def __init__(self, s3_client: S3Client) -> None:
        """Initialize with a boto3 S3 client."""
        self._s3 = s3_client

    def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
    ) -> S3GetObjectResponse:
        """Get object from S3."""
        request: GetObjectRequestTypeDef = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = self._s3.get_object(**request)
        return cast(S3GetObjectResponse, response)

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> S3PutObjectResponse:
        """Put object to S3."""
        request: PutObjectRequestTypeDef = {"Bucket": bucket, "Key": key, "Body": body}
        if content_type is not None:
            request["ContentType"] = content_type
        if if_match is not None:
            request["IfMatch"] = if_match
        if if_none_match is not None:
            request["IfNoneMatch"] = if_none_match
        if server_side_encryption is not None:
            request["ServerSideEncryption"] = server_side_encryption
        if sse_kms_key_id is not None:
            request["SSEKMSKeyId"] = sse_kms_key_id

        response = self._s3.put_object(**request)
        return cast(S3PutObjectResponse, response)

    def head_object(
        self,
        bucket: str,
        key: str,
    ) -> S3HeadObjectResponse:
        """Get object metadata from S3."""
        return cast(S3HeadObjectResponse, self._s3.head_object(Bucket=bucket, Key=key))

    def list_object_versions(
        self,
        bucket: str,
        prefix: str,
        key_marker: Optional[str] = None,
        version_id_marker: Optional[str] = None,
    ) -> S3ListObjectVersionsPage:
        """List object versions."""
        request: ListObjectVersionsRequestTypeDef = {"Bucket": bucket, "Prefix": prefix}
        if key_marker is not None:
            request["KeyMarker"] = key_marker
        if version_id_marker is not None:
            request["VersionIdMarker"] = version_id_marker

        response = self._s3.list_object_versions(**request)
        return cast(S3ListObjectVersionsPage, response)

    def get_paginator(self, operation_name: Literal["list_objects_v2"]) -> ListObjectsV2Paginator:
        """Get paginator for list operations."""
        return self._s3.get_paginator(operation_name)


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


# S3 response field extraction helpers


def log_version_id_from_get(response: S3GetObjectResponse) -> LogVersionId[K]:
    """Extract LogVersionId from GetObject response."""
    return LogVersionId(response["VersionId"])


def log_version_id_from_put(response: S3PutObjectResponse) -> LogVersionId[K]:
    """Extract LogVersionId from PutObject response."""
    return LogVersionId(response["VersionId"])


def log_version_id_from_head(response: S3HeadObjectResponse) -> LogVersionId[K]:
    """Extract LogVersionId from HeadObject response."""
    return LogVersionId(response["VersionId"])


def log_version_id_from_version(version: S3ObjectVersion) -> LogVersionId[K]:
    """Extract LogVersionId from S3ObjectVersion."""
    return LogVersionId(version["VersionId"])


def key_version_id_from_version(version: S3ObjectVersion) -> KeyVersionId[K]:
    """Extract KeyVersionId from S3ObjectVersion."""
    return KeyVersionId(version["VersionId"])


def key_object_etag_from_get(response: S3GetObjectResponse) -> KeyObjectETag[K]:
    """Extract KeyObjectETag from GetObject response."""
    return KeyObjectETag(response["ETag"])


def key_object_etag_from_put(response: S3PutObjectResponse) -> KeyObjectETag[K]:
    """Extract KeyObjectETag from PutObject response."""
    return KeyObjectETag(response["ETag"])


def key_object_etag_from_head(response: S3HeadObjectResponse) -> KeyObjectETag[K]:
    """Extract KeyObjectETag from HeadObject response."""
    return KeyObjectETag(response["ETag"])
