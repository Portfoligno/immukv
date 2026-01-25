"""Helper types for S3 file operations.

These types are not part of the public API and should only be used internally.
This module provides output type definitions for async S3 operations.
"""

from typing import Any, AsyncIterator, Generic, Mapping, Optional, TypedDict, TypeVar, cast

from immukv_files.types import FileVersionId
from immukv_files._internal.types import file_version_id_from_boto3

K = TypeVar("K", bound=str)
T = TypeVar("T")


class ErrorResponse(TypedDict):
    """Boto3 error response structure."""

    Code: str
    Message: str


class ClientErrorResponse(TypedDict):
    """Boto3 ClientError response structure."""

    Error: ErrorResponse


def get_error_code(error: Exception) -> str:
    """Extract error code from ClientError.

    Centralizes ClientError response access to satisfy disallow_any_expr.
    """
    error_response = cast(ClientErrorResponse, cast(Any, error).response)  # type: ignore[misc,explicit-any]
    return cast(str, error_response["Error"]["Code"])


# Type alias for aiobotocore responses (same structure as boto3, but Any typed)
AiobotocoreResponse = Mapping[str, Any]  # type: ignore[explicit-any]


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


class FilePutObjectOutput(TypedDict, Generic[K]):
    """PutObjectOutput with corrected field optionality for file operations."""

    etag: str
    version_id: Optional[str]


class FilePutObjectOutputs:
    """Helper functions for file PutObjectOutput."""

    @staticmethod
    def from_aiobotocore(response: AiobotocoreResponse) -> FilePutObjectOutput[K]:
        """Convert aiobotocore response to FilePutObjectOutput.

        Args:
            response: Raw aiobotocore put_object response

        Returns:
            FilePutObjectOutput with validated required fields
        """
        return {
            "etag": assert_aws_field_present(response.get("ETag"), "PutObjectOutput.ETag"),
            "version_id": response.get("VersionId"),
        }

    @staticmethod
    def file_version_id(response: FilePutObjectOutput[K]) -> Optional[FileVersionId[K]]:
        """Extract FileVersionId from PutObjectOutput."""
        version_id = response.get("version_id")
        if version_id is not None:
            return file_version_id_from_boto3(version_id)
        return None


class FileDeleteObjectOutput(TypedDict, Generic[K]):
    """DeleteObjectOutput with corrected field optionality for file operations.

    When deleting from a versioned bucket, S3 creates a "delete marker" instead of
    actually removing the object. The version_id in the response is the delete marker's
    version ID, not the original object's version ID.
    """

    delete_marker: Optional[bool]
    version_id: Optional[str]


class FileDeleteObjectOutputs:
    """Helper functions for file DeleteObjectOutput."""

    @staticmethod
    def from_aiobotocore(response: AiobotocoreResponse) -> FileDeleteObjectOutput[K]:
        """Convert aiobotocore response to FileDeleteObjectOutput.

        Args:
            response: Raw aiobotocore delete_object response

        Returns:
            FileDeleteObjectOutput with optional fields
        """
        return {
            "delete_marker": response.get("DeleteMarker"),
            "version_id": response.get("VersionId"),
        }

    @staticmethod
    def delete_marker_version_id(
        response: FileDeleteObjectOutput[K],
    ) -> Optional[FileVersionId[K]]:
        """Extract the delete marker's version ID from DeleteObjectOutput.

        When deleting from a versioned bucket without specifying a version ID,
        S3 creates a delete marker. The returned version_id is the delete marker's
        version ID, which can be used to reference this deletion event.
        """
        version_id = response.get("version_id")
        if version_id is not None:
            return file_version_id_from_boto3(version_id)
        return None


class FileGetObjectOutput(TypedDict, Generic[K]):
    """GetObjectOutput with corrected field optionality for async file operations."""

    body: AsyncIterator[bytes]
    etag: str
    version_id: Optional[str]
    content_length: int
    content_type: Optional[str]
    metadata: Optional[dict[str, str]]


class GetBucketVersioningOutput(TypedDict):
    """S3 GetBucketVersioning response with corrected field optionality."""

    Status: Optional[str]
    MFADelete: Optional[str]


class GetBucketVersioningOutputs:
    """Helper functions for GetBucketVersioningOutput."""

    @staticmethod
    def from_aiobotocore(response: AiobotocoreResponse) -> GetBucketVersioningOutput:
        """Convert aiobotocore response to GetBucketVersioningOutput.

        Args:
            response: Raw aiobotocore get_bucket_versioning response

        Returns:
            GetBucketVersioningOutput with optional fields
        """
        return {
            "Status": response.get("Status"),
            "MFADelete": response.get("MFADelete"),
        }


class HeadBucketOutput(TypedDict):
    """S3 HeadBucket response (minimal fields needed for validation)."""

    BucketLocationType: Optional[str]
    BucketLocationName: Optional[str]
    BucketRegion: Optional[str]
    AccessPointAlias: Optional[bool]


class HeadBucketOutputs:
    """Helper functions for HeadBucketOutput."""

    @staticmethod
    def from_aiobotocore(response: AiobotocoreResponse) -> HeadBucketOutput:
        """Convert aiobotocore response to HeadBucketOutput.

        Args:
            response: Raw aiobotocore head_bucket response

        Returns:
            HeadBucketOutput with optional fields
        """
        return {
            "BucketLocationType": response.get("BucketLocationType"),
            "BucketLocationName": response.get("BucketLocationName"),
            "BucketRegion": response.get("BucketRegion"),
            "AccessPointAlias": response.get("AccessPointAlias"),
        }


class FileGetObjectOutputs:
    """Helper functions for file GetObjectOutput."""

    @staticmethod
    def from_aiobotocore(
        response: AiobotocoreResponse,
        body_iterator: AsyncIterator[bytes],
    ) -> FileGetObjectOutput[K]:
        """Convert aiobotocore response to FileGetObjectOutput.

        Unlike the core package which reads body immediately for small JSON objects,
        file operations use streaming to handle large files efficiently.

        Args:
            response: Raw aiobotocore get_object response
            body_iterator: Async iterator over body chunks (caller manages stream lifecycle)

        Returns:
            FileGetObjectOutput with validated required fields and streaming body
        """
        return {
            "body": body_iterator,
            "etag": assert_aws_field_present(response.get("ETag"), "GetObjectOutput.ETag"),
            "version_id": response.get("VersionId"),
            "content_length": assert_aws_field_present(
                response.get("ContentLength"), "GetObjectOutput.ContentLength"
            ),
            "content_type": response.get("ContentType"),
            "metadata": response.get("Metadata"),
        }

    @staticmethod
    def file_version_id(response: FileGetObjectOutput[K]) -> Optional[FileVersionId[K]]:
        """Extract FileVersionId from GetObjectOutput."""
        version_id = response.get("version_id")
        if version_id is not None:
            return file_version_id_from_boto3(version_id)
        return None
