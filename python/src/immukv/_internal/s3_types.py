"""S3-specific type definitions for internal use.

These types are not part of the public API and should only be used internally.
"""

from typing import Dict, Generic, List, TypedDict, TypeVar

from immukv.types import KeyObjectETag, KeyVersionId, LogVersionId

# Type variables
K = TypeVar("K", bound=str)
V = TypeVar("V")


class LogKey(str):
    """Branded type for log file key to distinguish from regular keys."""

    pass


class S3KeyPath(str, Generic[K]):
    """S3 path string carrying the key type K for type safety."""

    def __new__(cls, value: str) -> "S3KeyPath[K]":
        return str.__new__(cls, value)  # type: ignore[return-value]


class S3KeyPaths:
    """Factory methods for creating S3 key paths."""

    @staticmethod
    def for_key(prefix: str, key: K) -> S3KeyPath[K]:
        """Create S3 path for a key object.

        Args:
            prefix: S3 key prefix (e.g., "prefix/")
            key: The key value

        Returns:
            S3 path for the key object file
        """
        return S3KeyPath[K](f"{prefix}keys/{key}.json")

    @staticmethod
    def for_log(prefix: str) -> "S3KeyPath[LogKey]":
        """Create S3 path for the log file.

        Args:
            prefix: S3 key prefix (e.g., "prefix/")

        Returns:
            S3 path for the log file
        """
        return S3KeyPath[LogKey](f"{prefix}_log.json")


# Response type definitions (only fields we actually use, no Any types)


class S3GetObjectResponse(TypedDict, Generic[K]):
    """S3 GetObject response with only used fields, parameterized by key type."""

    Body: object  # StreamingBody - opaque, we only call .read()
    ETag: str
    VersionId: str


class S3GetObjectResponses:
    """Namespace for S3GetObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "S3GetObjectResponse[LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from GetObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "S3GetObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from GetObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class S3PutObjectResponse(TypedDict, Generic[K]):
    """S3 PutObject response with only used fields, parameterized by key type."""

    ETag: str
    VersionId: str


class S3PutObjectResponses:
    """Namespace for S3PutObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "S3PutObjectResponse[LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from PutObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "S3PutObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from PutObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class S3HeadObjectResponse(TypedDict, Generic[K]):
    """S3 HeadObject response with only used fields, parameterized by key type."""

    ETag: str
    VersionId: str


class S3HeadObjectResponses:
    """Namespace for S3HeadObjectResponse helper functions."""

    @staticmethod
    def log_version_id(response: "S3HeadObjectResponse[LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from HeadObject response (for log operations)."""
        return LogVersionId(response["VersionId"])

    @staticmethod
    def key_object_etag(response: "S3HeadObjectResponse[K]") -> KeyObjectETag[K]:
        """Extract KeyObjectETag from HeadObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class S3ObjectVersion(TypedDict, Generic[K]):
    """S3 object version in list response, parameterized by key type."""

    Key: str
    VersionId: str
    IsLatest: bool
    ETag: str


class S3ObjectVersions:
    """Namespace for S3ObjectVersion helper functions."""

    @staticmethod
    def log_version_id(version: "S3ObjectVersion[LogKey]") -> LogVersionId[K]:
        """Extract LogVersionId from S3ObjectVersion (for log operations)."""
        return LogVersionId(version["VersionId"])

    @staticmethod
    def key_version_id(version: "S3ObjectVersion[K]") -> KeyVersionId[K]:
        """Extract KeyVersionId from S3ObjectVersion (for key operations)."""
        return KeyVersionId(version["VersionId"])


class S3ListObjectVersionsPage(TypedDict, Generic[K], total=False):
    """S3 ListObjectVersions response page, parameterized by key type."""

    Versions: "List[S3ObjectVersion[K]]"
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
