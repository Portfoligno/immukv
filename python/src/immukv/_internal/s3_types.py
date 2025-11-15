"""S3-specific type definitions for internal use.

These types are not part of the public API and should only be used internally.
"""

from typing import TYPE_CHECKING, Generic, List, TypedDict, TypeVar, Optional

from immukv.types import KeyObjectETag, KeyVersionId, LogVersionId

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import (
        GetObjectOutputTypeDef,
        HeadObjectOutputTypeDef,
        ListObjectVersionsOutputTypeDef,
        ObjectVersionTypeDef,
        PutObjectOutputTypeDef,
    )

# Type variables
K = TypeVar("K", bound=str)
V = TypeVar("V")
T = TypeVar("T")


def assert_aws_field_present(value: Optional[T], field_name: str) -> T:
    """Assert that a field marked optional by AWS SDK types is actually present.

    The boto3-stubs types incorrectly mark many fields as optional when they
    are always returned by AWS. This helper asserts that fields which are
    always returned by AWS are actually present at runtime.

    Args:
        value: The supposedly optional value
        field_name: Name of the field for error messages

    Returns:
        The value with None removed from type

    Raises:
        ValueError: If value is None (indicates AWS SDK bug or API change)
    """
    if value is None:
        raise ValueError(
            f"AWS SDK type bug: {field_name} is None but should always be present. "
            "This may indicate an AWS API change or SDK bug."
        )
    return value


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
    def for_log(prefix: str) -> S3KeyPath[LogKey]:
        """Create S3 path for the log file.

        Args:
            prefix: S3 key prefix (e.g., "prefix/")

        Returns:
            S3 path for the log file
        """
        return S3KeyPath[LogKey](f"{prefix}_log.json")


# Response type definitions (only fields we actually use, no Any types)


class GetObjectOutput(TypedDict, Generic[K]):
    """S3 GetObject response with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    Body: object  # StreamingBody - always returned per AWS docs
    ETag: str  # Always returned per AWS docs
    VersionId: Optional[str]  # Optional (absent when versioning disabled)


class GetObjectOutputs:
    """Namespace for GetObjectOutput helper functions."""

    @staticmethod
    def from_boto3(response: "GetObjectOutputTypeDef") -> GetObjectOutput[K]:
        """Convert boto3 GetObjectOutputTypeDef to our GetObjectOutput type.

        Reconstructs the response with correct field optionality, asserting
        that fields which should always be present are actually present.
        """
        return {
            "Body": assert_aws_field_present(response.get("Body"), "GetObjectOutput.Body"),
            "ETag": assert_aws_field_present(response.get("ETag"), "GetObjectOutput.ETag"),
            "VersionId": response.get("VersionId"),
        }

    @staticmethod
    def log_version_id(response: GetObjectOutput[LogKey]) -> Optional[LogVersionId[K]]:
        """Extract LogVersionId from GetObject response (for log operations)."""
        version_id = response.get("VersionId")
        return None if version_id is None else LogVersionId(version_id)

    @staticmethod
    def key_object_etag(response: GetObjectOutput[K]) -> KeyObjectETag[K]:
        """Extract KeyObjectETag from GetObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class PutObjectOutput(TypedDict, Generic[K]):
    """S3 PutObject response with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    ETag: str  # Always returned per AWS docs
    VersionId: Optional[str]  # Optional (absent when versioning disabled)


class PutObjectOutputs:
    """Namespace for PutObjectOutput helper functions."""

    @staticmethod
    def from_boto3(response: "PutObjectOutputTypeDef") -> PutObjectOutput[K]:
        """Convert boto3 PutObjectOutputTypeDef to our PutObjectOutput type.

        Reconstructs the response with correct field optionality, asserting
        that fields which should always be present are actually present.
        """
        return {
            "ETag": assert_aws_field_present(response.get("ETag"), "PutObjectOutput.ETag"),
            "VersionId": response.get("VersionId"),
        }

    @staticmethod
    def log_version_id(response: PutObjectOutput[LogKey]) -> Optional[LogVersionId[K]]:
        """Extract LogVersionId from PutObject response (for log operations)."""
        version_id = response.get("VersionId")
        return None if version_id is None else LogVersionId(version_id)

    @staticmethod
    def key_object_etag(response: PutObjectOutput[K]) -> KeyObjectETag[K]:
        """Extract KeyObjectETag from PutObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class HeadObjectOutput(TypedDict, Generic[K]):
    """S3 HeadObject response with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    ETag: str  # Always returned per AWS docs
    VersionId: Optional[str]  # Optional (absent when versioning disabled)


class HeadObjectOutputs:
    """Namespace for HeadObjectOutput helper functions."""

    @staticmethod
    def from_boto3(response: "HeadObjectOutputTypeDef") -> HeadObjectOutput[K]:
        """Convert boto3 HeadObjectOutputTypeDef to our HeadObjectOutput type.

        Reconstructs the response with correct field optionality, asserting
        that fields which should always be present are actually present.
        """
        return {
            "ETag": assert_aws_field_present(response.get("ETag"), "HeadObjectOutput.ETag"),
            "VersionId": response.get("VersionId"),
        }

    @staticmethod
    def log_version_id(response: HeadObjectOutput[LogKey]) -> Optional[LogVersionId[K]]:
        """Extract LogVersionId from HeadObject response (for log operations)."""
        version_id = response.get("VersionId")
        return None if version_id is None else LogVersionId(version_id)

    @staticmethod
    def key_object_etag(response: HeadObjectOutput[K]) -> KeyObjectETag[K]:
        """Extract KeyObjectETag from HeadObject response (for key operations)."""
        return KeyObjectETag(response["ETag"])


class ObjectVersion(TypedDict, Generic[K]):
    """S3 object version with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    Key: str  # Always returned per AWS docs
    VersionId: str  # Always returned per AWS docs
    IsLatest: bool  # Always returned per AWS docs
    ETag: str  # Always returned per AWS docs


class ObjectVersions:
    """Namespace for ObjectVersion helper functions."""

    @staticmethod
    def from_boto3(version: "ObjectVersionTypeDef") -> ObjectVersion[K]:
        """Convert boto3 ObjectVersionTypeDef to our ObjectVersion type.

        Reconstructs the object with correct field optionality, asserting
        that fields which should always be present are actually present.
        """
        return {
            "Key": assert_aws_field_present(version.get("Key"), "ObjectVersion.Key"),
            "VersionId": assert_aws_field_present(
                version.get("VersionId"), "ObjectVersion.VersionId"
            ),
            "IsLatest": assert_aws_field_present(version.get("IsLatest"), "ObjectVersion.IsLatest"),
            "ETag": assert_aws_field_present(version.get("ETag"), "ObjectVersion.ETag"),
        }

    @staticmethod
    def log_version_id(version: ObjectVersion[LogKey]) -> LogVersionId[K]:
        """Extract LogVersionId from ObjectVersion (for log operations)."""
        return LogVersionId(version["VersionId"])

    @staticmethod
    def key_version_id(version: ObjectVersion[K]) -> KeyVersionId[K]:
        """Extract KeyVersionId from ObjectVersion (for key operations)."""
        return KeyVersionId(version["VersionId"])


class ListObjectVersionsOutput(TypedDict, Generic[K]):
    """S3 ListObjectVersions response with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    Versions: Optional[List[ObjectVersion[K]]]  # Optional (can be empty)
    IsTruncated: bool  # Always returned per AWS docs
    NextKeyMarker: Optional[str]  # Optional (only when IsTruncated=true)
    NextVersionIdMarker: Optional[str]  # Optional (only when IsTruncated=true)


class ListObjectVersionsOutputs:
    """Namespace for ListObjectVersionsOutput helper functions."""

    @staticmethod
    def from_boto3(response: "ListObjectVersionsOutputTypeDef") -> ListObjectVersionsOutput[K]:
        """Convert boto3 ListObjectVersionsOutputTypeDef to our ListObjectVersionsOutput type.

        Reconstructs the response with correct field optionality, asserting
        that fields which should always be present are actually present.
        """
        versions = response.get("Versions")
        return {
            "IsTruncated": assert_aws_field_present(
                response.get("IsTruncated"), "ListObjectVersionsOutput.IsTruncated"
            ),
            "Versions": (
                None if versions is None else [ObjectVersions.from_boto3(v) for v in versions]
            ),
            "NextKeyMarker": response.get("NextKeyMarker"),
            "NextVersionIdMarker": response.get("NextVersionIdMarker"),
        }


class Object(TypedDict):
    """S3 object in list response."""

    Key: str


class ListObjectsV2Output(TypedDict):
    """S3 ListObjectsV2 response with corrected field optionality.

    AWS SDK types are incorrect due to boto3-stubs bugs.
    This type reflects actual AWS API behavior per documentation.
    """

    Contents: Optional[List[Object]]  # Optional (can be empty)
    IsTruncated: bool  # Always returned per AWS docs
    NextContinuationToken: Optional[str]  # Optional (only when IsTruncated=true)


class ErrorResponse(TypedDict):
    """Boto3 error response structure."""

    Code: str
    Message: str


class ClientErrorResponse(TypedDict):
    """Boto3 ClientError response structure."""

    Error: ErrorResponse
