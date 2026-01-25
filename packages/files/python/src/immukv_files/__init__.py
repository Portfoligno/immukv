"""ImmuKV File Storage - Async-only file storage with audit logging using ImmuKV."""

from immukv_files.client import (
    FileClient,
    create_file_client,
    file_value_decoder,
    file_value_encoder,
)
from immukv_files.types import (
    # Branded types
    ContentHash,
    FileVersionId,
    FileS3Key,
    # Config types
    FileStorageConfig,
    FileStorageConfigOverrides,
    # Metadata types
    FileMetadata,
    DeletedFileMetadata,
    FileValue,
    FileEntry,
    # Options types
    SetFileOptions,
    GetFileOptions,
    FileDownload,
    # Type guards
    is_deleted_file,
    is_active_file,
    # Error types (FileKeyNotFoundError is the new name)
    FileKeyNotFoundError,
    FileNotFoundError,  # Backwards compatibility alias
    FileDeletedError,
    IntegrityError,
    FileOrphanedError,
    ConfigurationError,
    MaxRetriesExceededError,
)

__version__ = "__VERSION_EeEyfbyVyf4JmFfk__"

__all__ = [
    # Client
    "FileClient",
    "create_file_client",
    # Codecs
    "file_value_decoder",
    "file_value_encoder",
    # Branded types
    "ContentHash",
    "FileVersionId",
    "FileS3Key",
    # Config types
    "FileStorageConfig",
    "FileStorageConfigOverrides",
    # Metadata types
    "FileMetadata",
    "DeletedFileMetadata",
    "FileValue",
    "FileEntry",
    # Options types
    "SetFileOptions",
    "GetFileOptions",
    "FileDownload",
    # Type guards
    "is_deleted_file",
    "is_active_file",
    # Error types
    "FileKeyNotFoundError",
    "FileNotFoundError",  # Backwards compatibility alias
    "FileDeletedError",
    "IntegrityError",
    "FileOrphanedError",
    "ConfigurationError",
    "MaxRetriesExceededError",
]
