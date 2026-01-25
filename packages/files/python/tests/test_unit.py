"""Unit tests for immukv-files async-only implementation."""

from typing import AsyncIterator

import pytest
import hashlib

from immukv_files.types import (
    ContentHash,
    FileMetadata,
    DeletedFileMetadata,
    FileS3Key,
    FileVersionId,
    FileDownload,
    FileKeyNotFoundError,
    FileNotFoundError,
    is_deleted_file,
    is_active_file,
)
from immukv_files._internal.types import (
    content_hash_from_digest,
    content_hash_from_json,
    is_valid_content_hash,
)
from immukv_files._internal.hashing import (
    compute_hash_from_bytes,
    compute_hash_from_async_iterator,
)


class TestContentHash:
    """Tests for ContentHash type and helpers."""

    def test_content_hash_from_digest(self) -> None:
        """Test creating ContentHash from hex digest."""
        digest = "a" * 64
        hash_val: ContentHash[str] = content_hash_from_digest(digest)
        assert str(hash_val) == f"sha256:{digest}"
        assert isinstance(hash_val, str)

    def test_content_hash_from_digest_invalid(self) -> None:
        """Test that invalid digest raises ValueError."""
        with pytest.raises(ValueError):
            content_hash_from_digest("abc")  # Too short

        with pytest.raises(ValueError):
            content_hash_from_digest("z" * 64)  # Invalid hex

    def test_content_hash_from_json(self) -> None:
        """Test parsing ContentHash from JSON string."""
        valid = "sha256:" + "a" * 64
        hash_val: ContentHash[str] = content_hash_from_json(valid)
        assert str(hash_val) == valid

    def test_content_hash_from_json_invalid(self) -> None:
        """Test that invalid JSON hash raises ValueError."""
        with pytest.raises(ValueError):
            content_hash_from_json("invalid")

        with pytest.raises(ValueError):
            content_hash_from_json("sha256:abc")  # Too short

    def test_is_valid_content_hash(self) -> None:
        """Test content hash validation."""
        valid = "sha256:" + "a" * 64
        assert is_valid_content_hash(valid) is True
        assert is_valid_content_hash("invalid") is False
        assert is_valid_content_hash("sha256:abc") is False


class TestHashComputation:
    """Tests for hash computation functions."""

    def test_compute_hash_from_bytes(self) -> None:
        """Test computing hash from bytes."""
        data = b"hello world"
        expected = hashlib.sha256(data).hexdigest()
        result: ContentHash[str] = compute_hash_from_bytes(data)
        assert str(result) == f"sha256:{expected}"

    @pytest.mark.asyncio
    async def test_compute_hash_from_async_iterator(self) -> None:
        """Test computing hash from async iterator."""
        data = b"hello world"
        expected = hashlib.sha256(data).hexdigest()

        async def async_gen() -> AsyncIterator[bytes]:
            yield b"hello "
            yield b"world"

        result: ContentHash[str]
        result, buffer, length = await compute_hash_from_async_iterator(async_gen())
        assert str(result) == f"sha256:{expected}"
        assert buffer == data
        assert length == len(data)


class TestFileMetadata:
    """Tests for FileMetadata dataclass."""

    def test_file_metadata_creation(self) -> None:
        """Test creating FileMetadata."""
        metadata: FileMetadata[str] = FileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            s3_version_id=FileVersionId[str]("v123"),
            content_hash=ContentHash[str]("sha256:" + "a" * 64),
            content_length=100,
            content_type="text/plain",
            user_metadata={"key": "value"},
        )
        assert metadata.s3_key == "files/test.txt"
        assert metadata.content_length == 100


class TestDeletedFileMetadata:
    """Tests for DeletedFileMetadata dataclass."""

    def test_deleted_metadata_creation(self) -> None:
        """Test creating DeletedFileMetadata."""
        tombstone: DeletedFileMetadata[str] = DeletedFileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            deleted_version_id=FileVersionId[str]("v456"),
            deleted=True,
        )
        assert tombstone.s3_key == "files/test.txt"
        assert tombstone.deleted is True


class TestTypeGuards:
    """Tests for type guard functions."""

    def test_is_deleted_file(self) -> None:
        """Test is_deleted_file type guard."""
        active: FileMetadata[str] = FileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            s3_version_id=FileVersionId[str]("v123"),
            content_hash=ContentHash[str]("sha256:" + "a" * 64),
            content_length=100,
            content_type="text/plain",
        )
        deleted: DeletedFileMetadata[str] = DeletedFileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            deleted_version_id=FileVersionId[str]("v456"),
            deleted=True,
        )

        assert is_deleted_file(active) is False
        assert is_deleted_file(deleted) is True

    def test_is_active_file(self) -> None:
        """Test is_active_file type guard."""
        active: FileMetadata[str] = FileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            s3_version_id=FileVersionId[str]("v123"),
            content_hash=ContentHash[str]("sha256:" + "a" * 64),
            content_length=100,
            content_type="text/plain",
        )
        deleted: DeletedFileMetadata[str] = DeletedFileMetadata(
            s3_key=FileS3Key[str]("files/test.txt"),
            deleted_version_id=FileVersionId[str]("v456"),
            deleted=True,
        )

        assert is_active_file(active) is True
        assert is_active_file(deleted) is False


class TestErrorTypes:
    """Tests for error types."""

    def test_file_key_not_found_error(self) -> None:
        """Test FileKeyNotFoundError."""
        error = FileKeyNotFoundError("File not found: test.txt")
        assert str(error) == "File not found: test.txt"
        assert isinstance(error, Exception)

    def test_backwards_compatibility_alias(self) -> None:
        """Test that FileNotFoundError is alias for FileKeyNotFoundError."""
        # The alias should be the same class
        assert FileNotFoundError is FileKeyNotFoundError

        # Catching one should catch the other
        try:
            raise FileKeyNotFoundError("test")
        except FileNotFoundError as e:
            assert str(e) == "test"
