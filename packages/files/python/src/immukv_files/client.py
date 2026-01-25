"""Async-only FileClient implementation for ImmuKV file storage."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    BinaryIO,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from botocore.exceptions import ClientError  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AioS3Client

from immukv import ImmuKVClient, Entry, KeyNotFoundError
from immukv.types import KeyVersionId
from immukv.json_helpers import JSONValue

from immukv_files.types import (
    ConfigurationError,
    ContentHash,
    DeletedFileMetadata,
    FileDeletedError,
    FileDownload,
    FileMetadata,
    FileKeyNotFoundError,
    FileS3Key,
    FileStorageConfig,
    FileValue,
    FileVersionId,
    GetFileOptions,
    MaxRetriesExceededError,
    SetFileOptions,
    is_deleted_file,
)
from immukv_files._internal.types import (
    content_hash_from_json,
    file_s3_key_for_file,
)
from immukv_files._internal.async_s3_client import AsyncFileS3Client
from immukv_files._internal.s3_helpers import FilePutObjectOutput, get_error_code

K = TypeVar("K", bound=str)


class FileClient(Generic[K]):
    """Async-only file storage client with ImmuKV audit logging.

    All public methods are async coroutines. No sync API is provided.

    Usage:
        async with ImmuKVClient.create(config, decoder, encoder) as kv:
            async with FileClient.create(kv) as files:
                entry = await files.set_file("doc.pdf", Path("/tmp/doc.pdf"))
                download = await files.get_file("doc.pdf")
                async for chunk in download.stream:
                    ...

    Three-phase write protocol:
        1. Upload file to S3 (async, compute hash during upload)
        2. Write log entry to ImmuKV (async, commit point)
        3. Write key object (async, handled by kv_client.set)

    Three-phase delete protocol:
        1. Delete S3 object (async, creates delete marker)
        2. Write tombstone to ImmuKV log (async)
        3. Update key object (async, handled by kv_client.set)
    """

    _kv_client: ImmuKVClient[K, FileValue[K]]
    _async_s3: AsyncFileS3Client[K]
    _file_bucket: str
    _file_prefix: str
    _kms_key_id: Optional[str]

    def __init__(
        self,
        kv_client: ImmuKVClient[K, FileValue[K]],
        async_s3: AsyncFileS3Client[K],
        file_bucket: str,
        file_prefix: str,
        kms_key_id: Optional[str] = None,
    ) -> None:
        """Initialize FileClient (internal - use create() factory)."""
        self._kv_client = kv_client
        self._async_s3 = async_s3
        self._file_bucket = file_bucket
        self._file_prefix = file_prefix
        self._kms_key_id = kms_key_id

    # =========================================================================
    # Factory Method
    # =========================================================================

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        kv_client: ImmuKVClient[K, FileValue[K]],
        config: Optional[FileStorageConfig] = None,
    ) -> AsyncIterator["FileClient[K]"]:
        """Create FileClient with managed lifecycle.

        Shares the aiobotocore session from the kv_client for efficiency.
        Validates bucket access and versioning before yielding.

        Args:
            kv_client: Async ImmuKV client for metadata operations
            config: Optional file storage configuration

        Yields:
            Configured FileClient ready for use

        Example:
            async with ImmuKVClient.create(config, decoder, encoder) as kv:
                async with FileClient.create(kv) as files:
                    await files.set_file("key", data)
        """
        # Get configuration from kv_client
        kv_config = kv_client._config

        # Determine file bucket and prefix
        file_bucket = config.bucket if config and config.bucket else kv_config.s3_bucket
        if config and config.bucket is not None:
            # Different bucket: default to no prefix
            file_prefix = config.prefix if config.prefix is not None else ""
        else:
            # Same bucket: default to "files/" prefix under kv_client prefix
            file_prefix = (
                config.prefix
                if config and config.prefix is not None
                else f"{kv_config.s3_prefix}files/"
            )

        kms_key_id = config.kms_key_id if config else None

        # Get the S3 client from kv_client
        # The kv_client should expose a method to get the aiobotocore client
        aio_s3_client: "AioS3Client" = kv_client._s3._s3
        async_s3: AsyncFileS3Client[K] = AsyncFileS3Client(aio_s3_client)

        client = cls(kv_client, async_s3, file_bucket, file_prefix, kms_key_id)

        # Validate bucket access and versioning
        validate_access = config.validate_access if config else True
        validate_versioning = config.validate_versioning if config else True

        if validate_access:
            await async_s3.head_bucket(file_bucket)

        if validate_versioning:
            await client._validate_bucket()

        yield client

    # =========================================================================
    # File Operations (all async)
    # =========================================================================

    async def set_file(
        self,
        key: K,
        source: Union[BinaryIO, bytes, str, Path],
        options: Optional[SetFileOptions] = None,
    ) -> Entry[K, FileValue[K]]:
        """Upload a file and record it in the audit log.

        Phase 1 (async): Upload file to S3, compute hash during transfer.
        Phase 2 (async): Write log entry to ImmuKV (commit point).
        Phase 3 (async): Key object write handled by kv_client.set.

        The S3 upload is cached for retry efficiency - if Phase 2 fails
        due to optimistic locking conflict, Phase 1 result is reused.

        Args:
            key: User-supplied file key
            source: File content as file object, bytes, string path, or Path
            options: Optional settings (content_type, user_metadata)

        Returns:
            The file entry with metadata

        Raises:
            MaxRetriesExceededError: If log write fails after max retries
            ConfigurationError: If S3 versioning is not enabled
        """
        data = self._read_source(source)
        s3_key: FileS3Key[K] = file_s3_key_for_file(self._file_prefix, key)
        content_type = options.content_type if options else "application/octet-stream"

        cached_upload: Optional[Tuple[FilePutObjectOutput[K], ContentHash[K]]] = None

        for attempt in range(10):
            if cached_upload is None:
                response, content_hash = await self._async_s3.put_object_with_hash(
                    bucket=self._file_bucket,
                    key=s3_key,
                    body=data,
                    content_type=content_type,
                    metadata=options.user_metadata if options else None,
                    sse_kms_key_id=self._kms_key_id,
                    server_side_encryption="aws:kms" if self._kms_key_id else None,
                )
                cached_upload = (response, content_hash)
            else:
                response, content_hash = cached_upload

            version_id = response.get("version_id")
            if version_id is None:
                raise ConfigurationError(
                    "S3 did not return version ID - versioning may be disabled"
                )

            metadata: FileMetadata[K] = FileMetadata(
                s3_key=s3_key,
                s3_version_id=FileVersionId(str(version_id)),
                content_hash=content_hash,
                content_length=len(data),
                content_type=content_type if content_type else "application/octet-stream",
                user_metadata=options.user_metadata if options else None,
            )

            try:
                entry = await self._kv_client.set(key, metadata)
                return entry
            except ClientError as e:  # type: ignore[misc]
                # Check for precondition failure (optimistic locking conflict)
                if get_error_code(e) == "PreconditionFailed":  # type: ignore[misc]
                    continue
                raise

        raise MaxRetriesExceededError(f"Failed to commit after 10 attempts: {key}")

    async def get_file(
        self,
        key: K,
        options: Optional[GetFileOptions[K]] = None,
    ) -> FileDownload[K]:
        """Get a file by key.

        Returns an async iterator for streaming file content along with metadata.

        Args:
            key: File key
            options: Optional version_id for historical access

        Returns:
            FileDownload with entry metadata and async stream iterator

        Raises:
            FileKeyNotFoundError: If key does not exist
            FileDeletedError: If file has been deleted
        """
        entry = await self._get_entry(key, options)

        if is_deleted_file(entry.value):
            raise FileDeletedError(f"File has been deleted: {key}")

        file_value: FileMetadata[K] = entry.value  # type: ignore[assignment]

        async def stream() -> AsyncIterator[bytes]:
            response = await self._async_s3.get_object(
                bucket=self._file_bucket,
                key=file_value.s3_key,
                version_id=file_value.s3_version_id,
            )
            body: AsyncIterator[bytes] = response["body"]
            # body is already an async iterator
            async for chunk in body:
                yield chunk

        return FileDownload(entry=entry, stream=stream())

    async def delete_file(self, key: K) -> Entry[K, FileValue[K]]:
        """Delete a file.

        Phase 1 (async): Delete S3 object (creates delete marker).
        Phase 2 (async): Write tombstone entry to log.
        Phase 3 (async): Key object handled by kv_client.set.

        Args:
            key: File key to delete

        Returns:
            The tombstone entry

        Raises:
            FileKeyNotFoundError: If key does not exist
            FileDeletedError: If file is already deleted
        """
        try:
            entry = await self._kv_client.get(key)
        except KeyNotFoundError as e:
            raise FileKeyNotFoundError(f"File not found: {key}") from e

        if entry is None:
            raise FileKeyNotFoundError(f"File not found: {key}")

        if is_deleted_file(entry.value):
            raise FileDeletedError(f"File already deleted: {key}")

        file_value: FileMetadata[K] = entry.value  # type: ignore[assignment]

        # Phase 1: S3 delete
        delete_response = await self._async_s3.delete_object(
            bucket=self._file_bucket,
            key=file_value.s3_key,
        )

        delete_marker_version_id = delete_response.get("version_id")
        if delete_marker_version_id is None:
            raise ConfigurationError("S3 delete did not return version ID")

        # Phase 2: Tombstone write
        tombstone: DeletedFileMetadata[K] = DeletedFileMetadata(
            s3_key=file_value.s3_key,
            deleted_version_id=FileVersionId(str(delete_marker_version_id)),
            deleted=True,
        )

        return await self._kv_client.set(key, tombstone)

    async def verify_file(self, entry: Entry[K, FileValue[K]]) -> bool:
        """Verify file integrity.

        Downloads the file and verifies content_hash matches.

        Args:
            entry: The file entry to verify

        Returns:
            True if computed hash matches stored hash
        """
        if not self._kv_client.verify(entry):
            return False

        if is_deleted_file(entry.value):
            return True  # Tombstones have no content to verify

        file_value: FileMetadata[K] = entry.value  # type: ignore[assignment]

        content, actual_hash = await self._async_s3.get_object_with_hash(
            bucket=self._file_bucket,
            key=file_value.s3_key,
            version_id=file_value.s3_version_id,
        )

        return str(actual_hash) == str(file_value.content_hash)

    # =========================================================================
    # Metadata Operations (all async - delegate to kv_client)
    # =========================================================================

    async def history(
        self,
        key: K,
        before_version_id: Optional[KeyVersionId[K]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[Entry[K, FileValue[K]]], Optional[KeyVersionId[K]]]:
        """Get history of a file key.

        Args:
            key: File key
            before_version_id: Pagination cursor
            limit: Maximum entries to return

        Returns:
            Tuple of (entries, next_cursor)
        """
        return await self._kv_client.history(key, before_version_id, limit)

    async def list_files(
        self,
        after_key: Optional[K] = None,
        limit: Optional[int] = None,
    ) -> List[K]:
        """List file keys.

        Args:
            after_key: Pagination cursor
            limit: Maximum keys to return

        Returns:
            List of file keys
        """
        return await self._kv_client.list_keys(after_key, limit)

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def kv_client(self) -> ImmuKVClient[K, FileValue[K]]:
        """The underlying ImmuKV client."""
        return self._kv_client

    # =========================================================================
    # Context Manager
    # =========================================================================

    async def __aenter__(self) -> "FileClient[K]":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass  # Session owned by kv_client

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    async def _get_entry(
        self,
        key: K,
        options: Optional[GetFileOptions[K]],
    ) -> Entry[K, FileValue[K]]:
        """Get entry, optionally by version."""
        if options and options.version_id:
            # Historical access via specific version
            history, _ = await self._kv_client.history(key, options.version_id, 1)
            if len(history) == 0:
                raise FileKeyNotFoundError(f"File '{key}' version '{options.version_id}' not found")
            return history[0]

        try:
            entry = await self._kv_client.get(key)
        except KeyNotFoundError as e:
            raise FileKeyNotFoundError(f"File not found: {key}") from e
        if entry is None:
            raise FileKeyNotFoundError(f"File not found: {key}")
        return entry

    async def _validate_bucket(self) -> None:
        """Validate bucket access and versioning."""
        versioning = await self._async_s3.get_bucket_versioning(self._file_bucket)
        if versioning.get("Status") != "Enabled":
            raise ConfigurationError(f"Bucket versioning not enabled: {self._file_bucket}")

    def _read_source(self, source: Union[BinaryIO, bytes, str, Path]) -> bytes:
        """Read source into bytes."""
        if isinstance(source, bytes):
            return source
        if isinstance(source, (str, Path)):
            path = Path(source)
            if path.exists():
                with open(path, "rb") as f:
                    return f.read()
            # Treat as string content (for str input only)
            if isinstance(source, str):
                return source.encode("utf-8")
            raise FileNotFoundError(f"File not found: {source}")
        return source.read()

    def _make_s3_key(self, key: K) -> str:
        """Generate S3 key path from user key."""
        return f"{self._file_prefix}{key}"


# =============================================================================
# Codec functions for file values
# =============================================================================


def file_value_decoder(json: JSONValue) -> FileValue[K]:
    """Value decoder for FileValue JSON.

    Used internally to parse file metadata from ImmuKV entries.

    Args:
        json: JSON value from ImmuKV

    Returns:
        Parsed FileValue
    """
    obj: dict[str, JSONValue] = json  # type: ignore[assignment]

    if obj.get("deleted") is True:
        # Tombstone
        return DeletedFileMetadata(
            s3_key=FileS3Key(str(obj["s3_key"])),
            deleted_version_id=FileVersionId(str(obj["deleted_version_id"])),
            deleted=True,
        )

    # Active file
    return FileMetadata(
        s3_key=FileS3Key(str(obj["s3_key"])),
        s3_version_id=FileVersionId(str(obj["s3_version_id"])),
        content_hash=content_hash_from_json(str(obj["content_hash"])),
        content_length=int(obj["content_length"]),  # type: ignore[arg-type]
        content_type=str(obj["content_type"]),
        user_metadata=obj.get("user_metadata"),  # type: ignore[arg-type]
    )


def file_value_encoder(value: FileValue[K]) -> JSONValue:
    """Value encoder for FileValue to JSON.

    Used internally to serialize file metadata for ImmuKV entries.

    Args:
        value: FileValue to serialize

    Returns:
        JSON-compatible dict
    """
    if is_deleted_file(value):
        deleted_value: DeletedFileMetadata[K] = value  # type: ignore[assignment]
        return {
            "deleted": True,
            "s3_key": str(deleted_value.s3_key),
            "deleted_version_id": str(deleted_value.deleted_version_id),
        }

    file_value: FileMetadata[K] = value  # type: ignore[assignment]
    result: dict[str, JSONValue] = {
        "s3_key": str(file_value.s3_key),
        "s3_version_id": str(file_value.s3_version_id),
        "content_hash": str(file_value.content_hash),
        "content_length": file_value.content_length,
        "content_type": file_value.content_type,
    }

    if file_value.user_metadata is not None:
        result["user_metadata"] = file_value.user_metadata  # type: ignore[assignment]

    return result


async def create_file_client(
    kv_client: ImmuKVClient[K, object],
    config: Optional[FileStorageConfig] = None,
) -> AsyncIterator[FileClient[K]]:
    """Create a FileClient from an ImmuKV client and configuration.

    This is an async context manager factory.
    Validates bucket access and versioning status.

    Args:
        kv_client: ImmuKV client (will be used with file codecs)
        config: Optional file storage configuration

    Yields:
        Validated FileClient

    Example:
        async with create_file_client(kv, config) as files:
            await files.set_file("key", data)
    """
    # Create typed client with file codecs
    typed_client: ImmuKVClient[K, FileValue[K]] = kv_client.with_codec(
        file_value_decoder, file_value_encoder
    )
    async with FileClient.create(typed_client, config) as client:
        yield client
