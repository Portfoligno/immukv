"""Internal type definitions not exposed in public API."""

import re
from typing import Generic, Optional, TypedDict, TypeVar

from immukv_files.types import ContentHash, FileS3Key, FileVersionId

K = TypeVar("K", bound=str)

# Regex pattern for valid content hash format
CONTENT_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


def content_hash_from_json(s: str) -> ContentHash[K]:
    """Parse content hash from JSON string with validation.

    Args:
        s: Hash string from JSON

    Returns:
        Validated ContentHash type

    Raises:
        ValueError: If hash format is invalid
    """
    if not s.startswith("sha256:") or len(s) != 71:
        raise ValueError(f"Invalid content hash format (expected 'sha256:' + 64 hex chars): {s}")
    return ContentHash(s)


def content_hash_from_digest(hex_digest: str) -> ContentHash[K]:
    """Create content hash from computed hex digest.

    Args:
        hex_digest: 64-character hex SHA-256 digest

    Returns:
        ContentHash with 'sha256:' prefix

    Raises:
        ValueError: If digest format is invalid
    """
    if len(hex_digest) != 64 or not re.match(r"^[0-9a-f]+$", hex_digest):
        raise ValueError(f"Invalid hex digest (expected 64 hex chars): {hex_digest}")
    return ContentHash(f"sha256:{hex_digest}")


def is_valid_content_hash(s: str) -> bool:
    """Check if a string is a valid content hash format.

    Args:
        s: String to validate

    Returns:
        True if valid content hash format
    """
    return bool(CONTENT_HASH_PATTERN.match(s))


def file_version_id_from_boto3(version_id: str) -> FileVersionId[K]:
    """Create FileVersionId from boto3 version ID.

    Args:
        version_id: S3 version ID from boto3

    Returns:
        Branded FileVersionId
    """
    return FileVersionId(version_id)


def file_version_id_from_json(s: str) -> FileVersionId[K]:
    """Parse FileVersionId from JSON string.

    Args:
        s: Version ID string from JSON

    Returns:
        Validated FileVersionId type
    """
    return FileVersionId(s)


def file_s3_key_for_file(prefix: str, key: str) -> FileS3Key[K]:
    """Create S3 path for a file object.

    Args:
        prefix: S3 key prefix (e.g., "files/")
        key: User-supplied file key

    Returns:
        Branded FileS3Key
    """
    return FileS3Key(f"{prefix}{key}")


class UploadResult(TypedDict, Generic[K]):
    """Result of file upload operation.

    Cached to prevent orphan multiplication on retry.
    """

    # S3 key where file was uploaded
    s3_key: FileS3Key[K]

    # S3 version ID assigned by S3
    s3_version_id: FileVersionId[K]

    # SHA-256 hash of uploaded content
    content_hash: ContentHash[K]

    # File size in bytes
    content_length: int

    # Content type
    content_type: str

    # User metadata if provided
    user_metadata: Optional[dict[str, str]]
