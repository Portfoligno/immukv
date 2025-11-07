"""Typed wrapper around boto3 S3 client to satisfy strict mypy checking.

This wrapper centralizes all type casts for boto3 S3 API responses,
allowing the main client code to work with Any-free types.
"""

import json
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

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
K_co = TypeVar("K_co", bound=str)  # Covariant K for method-level generics


# S3-specific branded types (internal implementation details)


class _LogKey(str):
    """Branded type for log file key to distinguish from regular keys."""

    pass


class _S3KeyPath(str, Generic[K]):
    """S3 path string carrying the key type K for type safety."""

    def __new__(cls, value: str) -> "_S3KeyPath[K]":
        return str.__new__(cls, value)  # type: ignore[return-value]


class _S3KeyPaths:
    """Factory methods for creating S3 key paths."""

    @staticmethod
    def for_key(prefix: str, key: K) -> _S3KeyPath[K]:
        """Create S3 path for a key object.

        Args:
            prefix: S3 key prefix (e.g., "prefix/")
            key: The key value

        Returns:
            S3 path for the key object file
        """
        return _S3KeyPath[K](f"{prefix}keys/{key}.json")

    @staticmethod
    def for_log(prefix: str) -> "_S3KeyPath[_LogKey]":
        """Create S3 path for the log file.

        Args:
            prefix: S3 key prefix (e.g., "prefix/")

        Returns:
            S3 path for the log file
        """
        return _S3KeyPath[_LogKey](f"{prefix}_log.json")


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


class _S3GetObjectResponse(TypedDict, Generic[K]):
    """S3 GetObject response with only used fields, parameterized by key type."""

    Body: object  # StreamingBody - opaque, we only call .read()
    ETag: str
    VersionId: str


class _S3GetObjectResponses:
    """Namespace for _S3GetObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "_S3GetObjectResponse[_LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from GetObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "_S3GetObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from GetObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class _S3PutObjectResponse(TypedDict, Generic[K]):
    """S3 PutObject response with only used fields, parameterized by key type."""

    ETag: str
    VersionId: str


class _S3PutObjectResponses:
    """Namespace for _S3PutObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "_S3PutObjectResponse[_LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from PutObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "_S3PutObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from PutObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class _S3HeadObjectResponse(TypedDict, Generic[K]):
    """S3 HeadObject response with only used fields, parameterized by key type."""

    ETag: str
    VersionId: str


class _S3HeadObjectResponses:
    """Namespace for _S3HeadObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "_S3HeadObjectResponse[_LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from HeadObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "_S3HeadObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from HeadObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class _S3ObjectVersion(TypedDict, Generic[K]):
    """S3 object version in list response, parameterized by key type."""

    Key: str
    VersionId: str
    IsLatest: bool
    ETag: str


class _S3ObjectVersions:
    """Namespace for S3ObjectVersion helper functions."""

    @staticmethod
    def log_version_id(version: "_S3ObjectVersion[_LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from S3ObjectVersion (for log operations)."""
        return LogVersionId(version["VersionId"])

    @staticmethod
    def key_version_id(version: "_S3ObjectVersion[K]") -> KeyVersionId[K]:
        """Extract KeyVersionId from S3ObjectVersion (for key operations)."""
        return KeyVersionId(version["VersionId"])


class _S3ListObjectVersionsPage(TypedDict, Generic[K], total=False):
    """S3 ListObjectVersions response page, parameterized by key type."""

    Versions: "List[_S3ObjectVersion[K]]"
    IsTruncated: bool
    NextKeyMarker: str
    NextVersionIdMarker: str


class _S3Object(TypedDict):
    """S3 object in list response."""

    Key: str


class _S3ListObjectsV2Page(TypedDict, total=False):
    """S3 ListObjectsV2 response page."""

    Contents: List[_S3Object]
    IsTruncated: bool
    NextContinuationToken: str


class _ErrorResponse(TypedDict):
    """Boto3 error response structure."""

    Code: str
    Message: str


class _ClientErrorResponse(TypedDict):
    """Boto3 ClientError response structure."""

    Error: _ErrorResponse


class _BrandedS3Client:
    """Branded S3 client wrapper returning nominally-typed responses.

    Centralizes all casts from boto3's Any-containing types to our
    clean Any-free type definitions. This allows strict mypy checking
    (disallow_any_expr) while working with boto3.
    """

    def __init__(self, s3_client: S3Client) -> None:
        """Initialize with a boto3 S3 client."""
        self._s3 = s3_client

    @overload
    def get_object(
        self,
        bucket: str,
        key: _S3KeyPath[_LogKey],
        version_id: Optional[str] = None,
    ) -> "_S3GetObjectResponse[_LogKey]": ...

    @overload
    def get_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
        version_id: Optional[str] = None,
    ) -> "_S3GetObjectResponse[K_co]": ...

    def get_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
        version_id: Optional[str] = None,
    ) -> "_S3GetObjectResponse[K_co]":
        """Get object from S3."""
        request: GetObjectRequestTypeDef = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = self._s3.get_object(**request)
        return cast("_S3GetObjectResponse[K_co]", response)

    @overload
    def put_object(
        self,
        bucket: str,
        key: _S3KeyPath[_LogKey],
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "_S3PutObjectResponse[_LogKey]": ...

    @overload
    def put_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "_S3PutObjectResponse[K_co]": ...

    def put_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "_S3PutObjectResponse[K_co]":
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
        return cast("_S3PutObjectResponse[K_co]", response)

    @overload
    def head_object(
        self,
        bucket: str,
        key: _S3KeyPath[_LogKey],
    ) -> "_S3HeadObjectResponse[_LogKey]": ...

    @overload
    def head_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
    ) -> "_S3HeadObjectResponse[K_co]": ...

    def head_object(
        self,
        bucket: str,
        key: "_S3KeyPath[K_co]",
    ) -> "_S3HeadObjectResponse[K_co]":
        """Get object metadata from S3."""
        return cast("_S3HeadObjectResponse[K_co]", self._s3.head_object(Bucket=bucket, Key=key))

    @overload
    def list_object_versions(
        self,
        bucket: str,
        prefix: _S3KeyPath[_LogKey],
        key_marker: Optional[_S3KeyPath[_LogKey]] = None,
        version_id_marker: Optional[str] = None,
    ) -> "_S3ListObjectVersionsPage[_LogKey]": ...

    @overload
    def list_object_versions(
        self,
        bucket: str,
        prefix: "_S3KeyPath[K_co]",
        key_marker: "Optional[_S3KeyPath[K_co]]" = None,
        version_id_marker: Optional[str] = None,
    ) -> "_S3ListObjectVersionsPage[K_co]": ...

    def list_object_versions(
        self,
        bucket: str,
        prefix: "_S3KeyPath[K_co]",
        key_marker: "Optional[_S3KeyPath[K_co]]" = None,
        version_id_marker: Optional[str] = None,
    ) -> "_S3ListObjectVersionsPage[K_co]":
        """List object versions."""
        request: ListObjectVersionsRequestTypeDef = {"Bucket": bucket, "Prefix": prefix}
        if key_marker is not None:
            request["KeyMarker"] = key_marker
        if version_id_marker is not None:
            request["VersionIdMarker"] = version_id_marker

        response = self._s3.list_object_versions(**request)
        return cast("_S3ListObjectVersionsPage[K_co]", response)

    def get_paginator(self, operation_name: Literal["list_objects_v2"]) -> ListObjectsV2Paginator:
        """Get paginator for list operations."""
        return self._s3.get_paginator(operation_name)


def _read_body_as_json(body: object) -> Dict[str, JSONValue]:
    """Read S3 Body object and parse as JSON.

    Centralizes json.loads() cast to satisfy disallow_any_expr.
    """
    body_data = cast(Any, body).read()  # type: ignore[misc,explicit-any]
    json_str = cast(str, body_data.decode("utf-8") if isinstance(body_data, bytes) else body_data)  # type: ignore[misc]
    return cast(Dict[str, JSONValue], json.loads(json_str))  # type: ignore[misc,explicit-any]


def _get_error_code(error: Exception) -> str:
    """Extract error code from ClientError.

    Centralizes ClientError response access to satisfy disallow_any_expr.
    """
    error_response = cast(_ClientErrorResponse, cast(Any, error).response)  # type: ignore[misc,explicit-any]
    return cast(str, error_response["Error"]["Code"])
