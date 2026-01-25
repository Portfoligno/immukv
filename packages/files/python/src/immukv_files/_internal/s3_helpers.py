"""Helper functions for S3 file operations.

These functions are not part of the public API and should only be used internally.
"""

from typing import TYPE_CHECKING, Generic, Iterator, Literal, Optional, TypedDict, TypeVar

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import (
        GetBucketVersioningOutputTypeDef,
        GetObjectOutputTypeDef,
        HeadBucketOutputTypeDef,
        PutObjectOutputTypeDef,
    )

from immukv_files.types import FileS3Key, FileVersionId
from immukv_files._internal.types import file_version_id_from_boto3

K = TypeVar("K", bound=str)


def assert_boto3_field_present(value: Optional[object], field_name: str) -> object:
    """Assert that a field marked optional by boto3 types is actually present.

    The boto3 TypeScript types incorrectly mark many fields as optional due to
    Smithy type generation bugs. This helper asserts that fields which are
    always returned by AWS are actually present at runtime.

    Args:
        value: The supposedly optional value
        field_name: Name of the field for error messages

    Returns:
        The value with None removed from type

    Raises:
        ValueError: If value is None (indicates boto3 bug or API change)
    """
    if value is None:
        raise ValueError(
            f"boto3 type bug: {field_name} is None but should always be present. "
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
    def from_boto3(response: "PutObjectOutputTypeDef") -> FilePutObjectOutput[K]:
        """Convert boto3 PutObjectOutput to our FilePutObjectOutput type."""
        etag = assert_boto3_field_present(response.get("ETag"), "PutObjectOutput.ETag")
        return {
            "etag": str(etag),
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
    def from_boto3(response: dict[str, object]) -> FileDeleteObjectOutput[K]:
        """Convert boto3 DeleteObjectOutput to our FileDeleteObjectOutput type."""
        return {
            "delete_marker": response.get("DeleteMarker"),  # type: ignore[typeddict-item]
            "version_id": response.get("VersionId"),  # type: ignore[typeddict-item]
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
    """GetObjectOutput with corrected field optionality for file operations."""

    body: Iterator[bytes]
    etag: str
    version_id: Optional[str]
    content_length: int
    content_type: Optional[str]
    metadata: Optional[dict[str, str]]


class FileGetObjectOutputs:
    """Helper functions for file GetObjectOutput."""

    @staticmethod
    def from_boto3(response: "GetObjectOutputTypeDef") -> FileGetObjectOutput[K]:
        """Convert boto3 GetObjectOutput to our FileGetObjectOutput type."""
        body = assert_boto3_field_present(response.get("Body"), "GetObjectOutput.Body")
        etag = assert_boto3_field_present(response.get("ETag"), "GetObjectOutput.ETag")
        content_length = assert_boto3_field_present(
            response.get("ContentLength"), "GetObjectOutput.ContentLength"
        )

        # Body is a StreamingBody, iterate over it
        # StreamingBody.iter_chunks() returns Iterator[bytes]
        streaming_body = body  # type: ignore[assignment]

        def body_iterator() -> Iterator[bytes]:
            """Iterate over streaming body in chunks."""
            # iter_chunks returns Iterator[bytes] but mypy doesn't see it
            for chunk in streaming_body.iter_chunks():  # type: ignore[misc,union-attr,attr-defined]
                yield chunk  # type: ignore[misc]

        return {
            "body": body_iterator(),
            "etag": str(etag),
            "version_id": response.get("VersionId"),
            "content_length": int(str(content_length)),
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


class FileS3Client:
    """Branded S3 client wrapper for file operations."""

    def __init__(self, s3_client: "S3Client") -> None:
        """Initialize with a boto3 S3 client."""
        self._s3 = s3_client

    def put_object(
        self,
        bucket: str,
        key: FileS3Key[K],
        body: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        sse_kms_key_id: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
    ) -> FilePutObjectOutput[K]:
        """Upload file to S3."""
        request: dict[str, object] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
        }
        if content_type is not None:
            request["ContentType"] = content_type
        else:
            request["ContentType"] = "application/octet-stream"
        if metadata is not None:
            request["Metadata"] = metadata
        if sse_kms_key_id is not None:
            request["SSEKMSKeyId"] = sse_kms_key_id
        if server_side_encryption is not None:
            request["ServerSideEncryption"] = server_side_encryption

        response: "PutObjectOutputTypeDef" = self._s3.put_object(**request)  # type: ignore[arg-type]
        return FilePutObjectOutputs.from_boto3(response)

    def get_object(
        self,
        bucket: str,
        key: FileS3Key[K],
        version_id: Optional[FileVersionId[K]] = None,
    ) -> FileGetObjectOutput[K]:
        """Download file from S3."""
        request: dict[str, object] = {
            "Bucket": bucket,
            "Key": key,
        }
        if version_id is not None:
            request["VersionId"] = version_id

        response: "GetObjectOutputTypeDef" = self._s3.get_object(**request)  # type: ignore[arg-type]
        return FileGetObjectOutputs.from_boto3(response)

    def delete_object(
        self,
        bucket: str,
        key: FileS3Key[K],
    ) -> FileDeleteObjectOutput[K]:
        """Delete file from S3.

        In a versioned bucket, this creates a delete marker rather than
        permanently removing the object. The returned version_id is the
        delete marker's version ID.
        """
        response = self._s3.delete_object(Bucket=bucket, Key=key)
        # Cast to dict for from_boto3 - TypedDict is compatible at runtime
        return FileDeleteObjectOutputs.from_boto3(dict(response))  # type: ignore[arg-type]

    def get_bucket_versioning(self, bucket: str) -> "GetBucketVersioningOutputTypeDef":
        """Check bucket versioning status."""
        return self._s3.get_bucket_versioning(Bucket=bucket)

    def head_bucket(self, bucket: str) -> "HeadBucketOutputTypeDef":
        """Head request to check bucket access."""
        return self._s3.head_bucket(Bucket=bucket)

    @property
    def client(self) -> "S3Client":
        """Direct access to underlying S3Client for operations not wrapped."""
        return self._s3
