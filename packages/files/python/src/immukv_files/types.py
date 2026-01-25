"""Type definitions for ImmuKV file storage."""

from dataclasses import dataclass
from typing import AsyncIterator, Generic, Optional, TypeVar, Union

from immukv.types import Entry, KeyVersionId, S3Credentials

# Type variables for generic key type
K = TypeVar("K", bound=str)


# Branded types parameterized by key type
# These are nominal types to prevent mixing values from different contexts


class ContentHash(str, Generic[K]):
    """SHA-256 content hash, prefixed with "sha256:".

    Key-parameterized to prevent cross-key hash confusion.
    Format: 'sha256:<64 hex characters>'
    """

    pass


class FileVersionId(str, Generic[K]):
    """S3 version ID for file objects.

    Distinct from LogVersionId and KeyVersionId to prevent accidental mixing.
    """

    pass


class FileS3Key(str, Generic[K]):
    """S3 key path for file objects.

    Type-safe path construction prevents path injection.
    """

    pass


@dataclass
class FileStorageConfigOverrides:
    """Override default S3 client behavior for file storage."""

    # Custom S3 endpoint URL
    endpoint_url: Optional[str] = None

    # Explicit credentials (not needed for AWS with IAM roles)
    credentials: Optional[S3Credentials] = None

    # Use path-style URLs instead of virtual-hosted style (required for MinIO)
    force_path_style: bool = False


@dataclass
class FileStorageConfig:
    """Configuration for file storage destination.

    If omitted entirely, files stored in same bucket as ImmuKV log.
    """

    # S3 bucket for files. If omitted, uses same bucket as ImmuKV log.
    bucket: Optional[str] = None

    # S3 region for files. If omitted, uses same region as log.
    region: Optional[str] = None

    # S3 key prefix for files. Default: "files/" for same bucket, "" for different bucket.
    prefix: Optional[str] = None

    # KMS key for file encryption.
    kms_key_id: Optional[str] = None

    # S3 client overrides (endpoint, credentials, pathStyle).
    overrides: Optional[FileStorageConfigOverrides] = None

    # Validate bucket access at construction. Default: True
    validate_access: bool = True

    # Validate bucket versioning is enabled. Default: True
    validate_versioning: bool = True


@dataclass
class FileMetadata(Generic[K]):
    """Metadata for an active file.

    Bucket is determined by FileClient configuration, not stored per-entry.
    """

    # S3 key within the configured bucket
    s3_key: FileS3Key[K]

    # S3 version ID for immutable reference
    s3_version_id: FileVersionId[K]

    # SHA-256 hash of file content (audit-grade integrity)
    content_hash: ContentHash[K]

    # File size in bytes
    content_length: int

    # MIME type
    content_type: str

    # Optional user-defined metadata
    user_metadata: Optional[dict[str, str]] = None


@dataclass
class DeletedFileMetadata(Generic[K]):
    """Metadata for a deleted file (tombstone).

    Use history() to see what content was deleted.
    """

    # S3 key within the configured bucket
    s3_key: FileS3Key[K]

    # S3 delete marker's version ID.
    #
    # When a file is deleted from a versioned S3 bucket, S3 creates a "delete marker"
    # instead of permanently removing the object. This field stores the delete marker's
    # version ID, not the original file's version ID. The original file content remains
    # accessible via S3 versioning for audit purposes.
    #
    # Use history() to find the original file's s3_version_id and content_hash.
    deleted_version_id: FileVersionId[K]

    # Discriminant: True for tombstones
    deleted: bool = True


# Union type for file value (active or deleted)
FileValue = Union[FileMetadata[K], DeletedFileMetadata[K]]

# Entry type for file operations
# Note: K is unbound at module level, but this is intentional for type alias usage
FileEntry = Entry[K, FileValue[K]]  # type: ignore[misc]


@dataclass
class SetFileOptions:
    """Options for set_file() operation."""

    # MIME type. If omitted, defaults to 'application/octet-stream'.
    content_type: Optional[str] = None

    # User-defined metadata to store with the file.
    user_metadata: Optional[dict[str, str]] = None


@dataclass
class GetFileOptions(Generic[K]):
    """Options for get_file() operation."""

    # Version ID for historical access. If omitted, returns latest active version.
    version_id: Optional[KeyVersionId[K]] = None


@dataclass
class FileDownload(Generic[K]):
    """Return type for get_file().

    Contains the file entry metadata and an async iterator
    for streaming file content.

    Usage:
        download = await client.get_file("key")
        async for chunk in download.stream:
            process(chunk)
    """

    # The file entry metadata from the log.
    entry: Entry[K, FileValue[K]]

    # Async iterator of file content chunks (bytes).
    stream: AsyncIterator[bytes]


def is_deleted_file(value: FileValue[K]) -> bool:
    """Type guard to check if a file value represents a deleted file.

    Args:
        value: File value to check

    Returns:
        True if value is a DeletedFileMetadata (tombstone)
    """
    return isinstance(value, DeletedFileMetadata) and value.deleted is True


def is_active_file(value: FileValue[K]) -> bool:
    """Type guard to check if a file value represents an active file.

    Args:
        value: File value to check

    Returns:
        True if value is a FileMetadata (active file)
    """
    return isinstance(value, FileMetadata)


class FileKeyNotFoundError(Exception):
    """Error thrown when a file key is not found.

    Renamed from FileNotFoundError to avoid shadowing Python's
    builtin FileNotFoundError (an OSError subclass).
    """

    pass


# Backwards compatibility alias (deprecated)
FileNotFoundError = FileKeyNotFoundError


class FileDeletedError(Exception):
    """Error thrown when accessing a deleted file."""

    pass


class IntegrityError(Exception):
    """Error thrown when file content hash does not match."""

    pass


class FileOrphanedError(Exception):
    """Error thrown when file exists in log but S3 version is missing."""

    pass


class ConfigurationError(Exception):
    """Error thrown for invalid configuration."""

    pass


class MaxRetriesExceededError(Exception):
    """Error thrown when set_file() retry limit is reached."""

    pass
