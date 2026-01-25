"""FileClient implementation for ImmuKV file storage."""

from pathlib import Path
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from immukv import ImmuKVClient, Config as ImmuKVConfig, KeyVersionId, Entry
from immukv.json_helpers import JSONValue

from immukv_files.types import (
    ConfigurationError,
    ContentHash,
    DeletedFileMetadata,
    FileDeletedError,
    FileDownload,
    FileMetadata,
    FileNotFoundError,
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
    UploadResult,
    content_hash_from_json,
    file_s3_key_for_file,
)
from immukv_files._internal.hashing import (
    compute_hash_from_bytes,
    compute_hash_from_file,
    compute_hash_from_path,
)
from immukv_files._internal.s3_helpers import (
    FileS3Client,
    FilePutObjectOutputs,
    FileDeleteObjectOutputs,
)

K = TypeVar("K", bound=str)


class FileClient(Generic[K]):
    """FileClient for storing and retrieving files with ImmuKV audit logging.

    Uses three-phase write protocol:
    1. Upload file to S3 (compute hash during upload)
    2. Write log entry to ImmuKV (commit point)
    3. Write key object for fast lookup (best effort)
    """

    # Instance field type annotations
    _kv_client: ImmuKVClient[K, FileValue[K]]
    _file_s3: FileS3Client
    _owns_s3_client: bool
    _file_bucket: str
    _file_prefix: str
    _kms_key_id: Optional[str]

    def __init__(
        self,
        kv_client: ImmuKVClient[K, FileValue[K]],
        config: Optional[FileStorageConfig] = None,
    ) -> None:
        """Create a FileClient.

        For most use cases, prefer the static `create()` factory method
        which validates bucket access and versioning.

        Args:
            kv_client: ImmuKV client for log operations
            config: Optional file storage configuration
        """
        self._kv_client = kv_client

        # Access internal config from kv_client
        kv_config: ImmuKVConfig = kv_client._config  # type: ignore[attr-defined]

        # Determine S3 client sharing
        same_region = (config.region if config else None) is None or (
            config is not None and config.region == kv_config.s3_region
        )
        same_overrides = config is None or config.overrides is None
        same_bucket = config is None or config.bucket is None

        if same_bucket and same_region and same_overrides:
            # Share S3 client from kv_client
            self._file_s3 = FileS3Client(kv_client._s3._s3)  # type: ignore[attr-defined]
            self._owns_s3_client = False
        else:
            # Create separate S3 client
            client_params: dict[str, object] = {
                "region_name": config.region if config and config.region else kv_config.s3_region,
            }

            if config and config.overrides:
                if config.overrides.endpoint_url:
                    client_params["endpoint_url"] = config.overrides.endpoint_url
                if config.overrides.credentials:
                    client_params["aws_access_key_id"] = (
                        config.overrides.credentials.aws_access_key_id
                    )
                    client_params["aws_secret_access_key"] = (
                        config.overrides.credentials.aws_secret_access_key
                    )
                if config.overrides.force_path_style:
                    client_params["config"] = BotocoreConfig(s3={"addressing_style": "path"})

            raw_s3: "S3Client" = boto3.client("s3", **client_params)  # type: ignore[assignment,call-overload]
            self._file_s3 = FileS3Client(raw_s3)
            self._owns_s3_client = True

        # Determine file bucket and prefix
        self._file_bucket = config.bucket if config and config.bucket else kv_config.s3_bucket
        if config and config.bucket is not None:
            # Different bucket: default to no prefix
            self._file_prefix = config.prefix if config.prefix is not None else ""
        else:
            # Same bucket: default to "files/" prefix under kv_client prefix
            self._file_prefix = (
                config.prefix
                if config and config.prefix is not None
                else f"{kv_config.s3_prefix}files/"
            )

        self._kms_key_id = config.kms_key_id if config else None

    @classmethod
    def create(
        cls,
        kv_client: ImmuKVClient[K, FileValue[K]],
        config: Optional[FileStorageConfig] = None,
    ) -> "FileClient[K]":
        """Create a FileClient with validation.

        Validates bucket access and versioning status before returning.
        This is the recommended way to create a FileClient.

        Args:
            kv_client: ImmuKV client for log operations
            config: Optional file storage configuration

        Returns:
            Validated FileClient

        Raises:
            ConfigurationError: If bucket is inaccessible or versioning disabled
        """
        client: FileClient[K] = cls(kv_client, config)

        validate_access = config.validate_access if config else True
        validate_versioning = config.validate_versioning if config else True

        if validate_access:
            try:
                client._file_s3.head_bucket(client._file_bucket)
            except ClientError as e:  # type: ignore[misc]
                raise ConfigurationError(
                    f"Cannot access file bucket '{client._file_bucket}': {e}"
                ) from e

        if validate_versioning:
            client._validate_versioning()

        return client

    @property
    def kv_client(self) -> ImmuKVClient[K, FileValue[K]]:
        """The underlying ImmuKV client for log operations."""
        return self._kv_client

    def set_file(
        self,
        key: K,
        source: Union[BinaryIO, bytes, str, Path],
        options: Optional[SetFileOptions] = None,
    ) -> "Entry[K, FileValue[K]]":
        """Upload a file and record it in the log.

        Three-phase write protocol:
        1. Upload file to S3 (compute hash during upload)
        2. Write log entry to ImmuKV (commit point)
        3. Write key object for fast lookup (best effort)

        Args:
            key: User-supplied file key
            source: File content as file object, bytes, string path, or Path
            options: Optional settings (content_type, user_metadata)

        Returns:
            The file entry

        Raises:
            MaxRetriesExceededError: If log write fails after retries
        """
        max_retries = 10
        upload_result: Optional[UploadResult[K]] = None

        for attempt in range(max_retries):
            # Phase 1: Upload file only once (cache result for retries)
            if upload_result is None:
                upload_result = self._upload_file(key, source, options)

            try:
                # Phase 2: Write log entry (may conflict on concurrent writes)
                metadata = FileMetadata(
                    s3_key=upload_result["s3_key"],
                    s3_version_id=upload_result["s3_version_id"],
                    content_hash=upload_result["content_hash"],
                    content_length=upload_result["content_length"],
                    content_type=upload_result["content_type"],
                    user_metadata=upload_result["user_metadata"],
                )

                # Use kv_client.set() which handles log write with optimistic locking
                entry = self._kv_client.set(key, metadata)  # type: ignore[arg-type]

                # Phase 3: Key object write is handled by kv_client.set() internally

                return entry

            except ClientError as e:  # type: ignore[misc]
                # Retry on precondition failure (concurrent write conflict)
                error_response: dict[str, object] = e.response  # type: ignore[misc,assignment]
                error_dict = error_response.get("Error", {})
                error_code = error_dict.get("Code", "") if isinstance(error_dict, dict) else ""  # type: ignore[misc]
                response_meta = error_response.get("ResponseMetadata", {})
                http_status: object = response_meta.get("HTTPStatusCode") if isinstance(response_meta, dict) else None  # type: ignore[misc]
                if error_code == "PreconditionFailed" or http_status == 412:  # type: ignore[misc]
                    # DO NOT re-upload - reuse cached upload_result
                    continue
                raise

        raise MaxRetriesExceededError(
            f"Failed to write file entry for '{key}' after {max_retries} retries"
        )

    def get_file(self, key: K, options: Optional[GetFileOptions[K]] = None) -> FileDownload[K]:
        """Get a file by key.

        Returns the file content as an iterator along with metadata.
        Supports historical access via version_id option.

        Args:
            key: File key
            options: Optional settings (version_id for historical access)

        Returns:
            FileDownload with entry and stream iterator

        Raises:
            FileNotFoundError: If key does not exist
            FileDeletedError: If file has been deleted
        """
        # Get entry from log
        entry: Entry[K, FileValue[K]]

        if options and options.version_id is not None:
            # Historical access via specific version
            history, _ = self._kv_client.history(key, options.version_id, 1)
            if len(history) == 0:
                raise FileNotFoundError(f"File '{key}' version '{options.version_id}' not found")
            entry = history[0]
        else:
            # Current version
            try:
                entry = self._kv_client.get(key)
            except Exception as e:
                if "KeyNotFoundError" in str(type(e)):
                    # Fallback: check log history (key object might be missing)
                    history, _ = self._kv_client.history(key, None, 1)
                    if len(history) == 0:
                        raise FileNotFoundError(f"File '{key}' not found") from e
                    entry = history[0]
                else:
                    raise

        # Check if deleted
        if is_deleted_file(entry.value):
            raise FileDeletedError(f"File '{key}' has been deleted")

        # Download file from S3
        file_value: FileMetadata[K] = entry.value  # type: ignore[assignment]
        response = self._file_s3.get_object(
            bucket=self._file_bucket,
            key=file_value.s3_key,
            version_id=file_value.s3_version_id,
        )

        return FileDownload(
            entry=entry,
            stream=response["body"],
        )

    def get_file_to_path(
        self,
        key: K,
        dest_path: Union[str, Path],
        options: Optional[GetFileOptions[K]] = None,
    ) -> "Entry[K, FileValue[K]]":
        """Get a file and write it to a local path.

        Args:
            key: File key
            dest_path: Destination file path
            options: Optional settings (version_id for historical access)

        Returns:
            The file entry

        Raises:
            FileNotFoundError: If key does not exist
            FileDeletedError: If file has been deleted
        """
        download = self.get_file(key, options)

        # Ensure directory exists
        path = Path(dest_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write stream to file
        with path.open("wb") as f:
            for chunk in download.stream:
                f.write(chunk)

        return download.entry

    def delete_file(self, key: K) -> "Entry[K, FileValue[K]]":
        """Delete a file.

        Three-phase delete protocol:
        1. Delete S3 object (creates delete marker in versioned bucket)
        2. Write tombstone entry to log with delete marker's version ID
        3. Update key object (handled by kv_client.set)

        The original file content remains accessible via S3 versioning for audit purposes.
        Use history() to see deleted files and their original content hashes.

        Args:
            key: File key to delete

        Returns:
            The tombstone entry

        Raises:
            FileNotFoundError: If key does not exist
            FileDeletedError: If file is already deleted
            ConfigurationError: If S3 delete does not return a version ID
        """
        # Get current entry to verify it exists and is not deleted
        try:
            current_entry = self._kv_client.get(key)
        except Exception as e:
            if "KeyNotFoundError" in str(type(e)):
                raise FileNotFoundError(f"File '{key}' not found") from e
            raise

        if is_deleted_file(current_entry.value):
            raise FileDeletedError(f"File '{key}' is already deleted")

        current_value: FileMetadata[K] = current_entry.value  # type: ignore[assignment]

        # Phase 1: Delete S3 object (creates delete marker in versioned bucket)
        delete_response = self._file_s3.delete_object(
            bucket=self._file_bucket,
            key=current_value.s3_key,
        )

        delete_marker_version_id: Optional[FileVersionId[K]] = (
            FileDeleteObjectOutputs.delete_marker_version_id(delete_response)
        )  # type: ignore[misc]
        if delete_marker_version_id is None:
            raise ConfigurationError(
                f"S3 DeleteObject response missing VersionId - ensure versioning is "
                f"enabled on bucket '{self._file_bucket}'"
            )

        # Phase 2: Write tombstone entry with delete marker's version ID
        tombstone: DeletedFileMetadata[K] = DeletedFileMetadata(
            s3_key=current_value.s3_key,
            deleted_version_id=delete_marker_version_id,  # type: ignore[misc]
            deleted=True,
        )  # type: ignore[misc]

        # Phase 3: Key object update is handled by kv_client.set internally
        return self._kv_client.set(key, tombstone)  # type: ignore[arg-type,misc]

    def history(
        self,
        key: K,
        before_version_id: Optional[KeyVersionId[K]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List["Entry[K, FileValue[K]]"], Optional[KeyVersionId[K]]]:
        """Get history of a file key.

        Returns all entries including deletions, newest first.

        Args:
            key: File key
            before_version_id: Pagination cursor (pass last version_id from previous result)
            limit: Maximum entries to return

        Returns:
            Tuple of (entries, next_cursor)
        """
        return self._kv_client.history(key, before_version_id, limit)

    def list_files(self, after_key: Optional[K] = None, limit: Optional[int] = None) -> List[K]:
        """List file keys.

        Returns keys in lexicographic order.
        Pass the last key from the previous result as `after_key` for pagination.

        Args:
            after_key: Pagination cursor
            limit: Maximum keys to return

        Returns:
            List of keys
        """
        return self._kv_client.list_keys(after_key, limit)

    def verify_file(self, entry: "Entry[K, FileValue[K]]") -> bool:
        """Verify file integrity by downloading and checking content hash.

        For active files: downloads file and verifies content_hash.
        For tombstones: verifies entry hash only (no content to verify).

        Args:
            entry: File entry to verify

        Returns:
            True if integrity check passes
        """
        # First verify entry hash (metadata integrity)
        entry_valid = self._kv_client.verify(entry)  # type: ignore[misc]
        if not entry_valid:
            return False

        # For tombstones, entry hash verification is sufficient
        if is_deleted_file(entry.value):  # type: ignore[misc]
            return True

        # For active files, download and verify content hash
        file_value: FileMetadata[K] = entry.value  # type: ignore[assignment,misc]

        try:
            response = self._file_s3.get_object(
                bucket=self._file_bucket,
                key=file_value.s3_key,
                version_id=file_value.s3_version_id,
            )

            # Stream to buffer and compute hash
            chunks: list[bytes] = []
            for chunk in response["body"]:
                chunks.append(chunk)
            buffer = b"".join(chunks)

            actual_hash: ContentHash[K] = compute_hash_from_bytes(buffer)  # type: ignore[misc]
            return str(actual_hash) == str(file_value.content_hash)

        except ClientError as e:  # type: ignore[misc]
            error_response: dict[str, object] = e.response  # type: ignore[misc,assignment]
            error_dict = error_response.get("Error", {})
            error_code = error_dict.get("Code", "") if isinstance(error_dict, dict) else ""
            if error_code in ["NoSuchKey", "NoSuchVersion"]:
                # File missing from S3
                return False
            raise

    def close(self) -> None:
        """Close the client and cleanup resources.

        Only destroys S3 client if it was created by this FileClient.
        The underlying kv_client is not closed - caller is responsible for that.
        """
        if self._owns_s3_client:
            # boto3 clients don't have a close method, but we can clear references
            pass

    def __enter__(self) -> "FileClient[K]":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[object],
    ) -> None:
        """Context manager exit."""
        self.close()

    # Private helper methods

    def _upload_file(
        self,
        key: K,
        source: Union[BinaryIO, bytes, str, Path],
        options: Optional[SetFileOptions],
    ) -> UploadResult[K]:
        """Upload file to S3 and compute hash."""
        s3_key: FileS3Key[K] = file_s3_key_for_file(self._file_prefix, key)
        content_type = options.content_type if options else None
        if content_type is None:
            content_type = "application/octet-stream"

        # Prepare content and compute hash
        buffer: bytes
        content_hash: ContentHash[K]
        content_length: int

        if isinstance(source, bytes):
            buffer = source
            content_hash = compute_hash_from_bytes(source)
            content_length = len(source)
        elif isinstance(source, (str, Path)):
            path = Path(source)
            if path.exists():
                content_hash, buffer, content_length = compute_hash_from_path(path)
            else:
                # Treat as string content (for str input only)
                if isinstance(source, str):
                    buffer = source.encode("utf-8")
                    content_hash = compute_hash_from_bytes(buffer)
                    content_length = len(buffer)
                else:
                    raise FileNotFoundError(f"File not found: {source}")
        else:
            # BinaryIO - read and hash
            content_hash, buffer, content_length = compute_hash_from_file(source)

        # Upload to S3
        put_response = self._file_s3.put_object(
            bucket=self._file_bucket,
            key=s3_key,
            body=buffer,
            content_type=content_type,
            metadata=options.user_metadata if options else None,
            sse_kms_key_id=self._kms_key_id,
            server_side_encryption="aws:kms" if self._kms_key_id else None,
        )

        s3_version_id: Optional[FileVersionId[K]] = FilePutObjectOutputs.file_version_id(
            put_response
        )  # type: ignore[misc]
        if s3_version_id is None:
            raise ConfigurationError(
                f"S3 PutObject response missing VersionId - ensure versioning is enabled "
                f"on bucket '{self._file_bucket}'"
            )

        return {
            "s3_key": s3_key,
            "s3_version_id": s3_version_id,
            "content_hash": content_hash,
            "content_length": content_length,
            "content_type": content_type,
            "user_metadata": options.user_metadata if options else None,
        }

    def _validate_versioning(self) -> None:
        """Validate that file bucket has versioning enabled."""
        response = self._file_s3.get_bucket_versioning(self._file_bucket)

        status = response.get("Status")
        if status != "Enabled":
            raise ConfigurationError(
                f"File bucket '{self._file_bucket}' must have versioning enabled. "
                f"Current: {status or 'Disabled'}. "
                f"Enable with: aws s3api put-bucket-versioning --bucket {self._file_bucket} "
                f"--versioning-configuration Status=Enabled"
            )


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


def create_file_client(
    kv_client: ImmuKVClient[K, object],
    config: Optional[FileStorageConfig] = None,
) -> FileClient[K]:
    """Create a FileClient from an ImmuKV client and configuration.

    This is the recommended way to create a FileClient.
    Validates bucket access and versioning status.

    Args:
        kv_client: ImmuKV client (will be used with file codecs)
        config: Optional file storage configuration

    Returns:
        Validated FileClient
    """
    # Create typed client with file codecs
    typed_client: ImmuKVClient[K, FileValue[K]] = kv_client.with_codec(
        file_value_decoder, file_value_encoder
    )
    return FileClient.create(typed_client, config)
