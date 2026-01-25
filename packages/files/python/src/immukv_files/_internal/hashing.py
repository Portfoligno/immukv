"""SHA-256 streaming hash computation for file content."""

import hashlib
from pathlib import Path
from typing import AsyncIterator, BinaryIO, Iterator, Tuple, TypeVar, Union

from immukv_files.types import ContentHash
from immukv_files._internal.types import content_hash_from_digest

K = TypeVar("K", bound=str)


def compute_hash_from_bytes(data: bytes) -> ContentHash[K]:
    """Compute SHA-256 hash of bytes.

    Args:
        data: Bytes to hash

    Returns:
        ContentHash with 'sha256:' prefix
    """
    hash_obj = hashlib.sha256()
    hash_obj.update(data)
    hex_digest = hash_obj.hexdigest()
    return content_hash_from_digest(hex_digest)


def compute_hash_from_file(file_obj: BinaryIO) -> Tuple[ContentHash[K], bytes, int]:
    """Compute SHA-256 hash of a file object.

    Reads the entire file and buffers it for upload.

    Args:
        file_obj: Binary file object to hash

    Returns:
        Tuple of (content_hash, buffer, content_length)
    """
    hash_obj = hashlib.sha256()
    chunks: list[bytes] = []

    while True:
        chunk = file_obj.read(65536)  # 64KB chunks
        if not chunk:
            break
        hash_obj.update(chunk)
        chunks.append(chunk)

    buffer = b"".join(chunks)
    hex_digest = hash_obj.hexdigest()
    return content_hash_from_digest(hex_digest), buffer, len(buffer)


def compute_hash_from_path(file_path: Union[str, Path]) -> Tuple[ContentHash[K], bytes, int]:
    """Compute SHA-256 hash of a file at the given path.

    Reads the entire file and buffers it for upload.

    Args:
        file_path: Path to file to hash

    Returns:
        Tuple of (content_hash, buffer, content_length)
    """
    path = Path(file_path)
    with path.open("rb") as f:
        return compute_hash_from_file(f)


def compute_hash_from_iterator(data: Iterator[bytes]) -> Tuple[ContentHash[K], bytes, int]:
    """Compute SHA-256 hash from an iterator of bytes.

    Consumes the iterator and buffers content for upload.

    Args:
        data: Iterator of bytes chunks

    Returns:
        Tuple of (content_hash, buffer, content_length)
    """
    hash_obj = hashlib.sha256()
    chunks: list[bytes] = []

    for chunk in data:
        hash_obj.update(chunk)
        chunks.append(chunk)

    buffer = b"".join(chunks)
    hex_digest = hash_obj.hexdigest()
    return content_hash_from_digest(hex_digest), buffer, len(buffer)


def verify_bytes_hash(data: bytes, expected_hash: ContentHash[K]) -> bool:
    """Verify that bytes match an expected content hash.

    Args:
        data: Bytes to verify
        expected_hash: Expected ContentHash

    Returns:
        True if hash matches
    """
    actual_hash: ContentHash[str] = compute_hash_from_bytes(data)
    return str(actual_hash) == str(expected_hash)


def verify_iterator_hash(data: Iterator[bytes], expected_hash: ContentHash[K]) -> bool:
    """Verify that iterator content matches an expected content hash.

    Args:
        data: Iterator of bytes to verify
        expected_hash: Expected ContentHash

    Returns:
        True if hash matches
    """
    actual_hash: ContentHash[str]
    actual_hash, _, _ = compute_hash_from_iterator(data)
    return str(actual_hash) == str(expected_hash)


# =============================================================================
# Async hash functions
# =============================================================================


async def compute_hash_from_async_iterator(
    data: AsyncIterator[bytes],
) -> Tuple[ContentHash[str], bytes, int]:
    """Compute SHA-256 hash from async iterator.

    Consumes the iterator and buffers content.

    Args:
        data: Async iterator of byte chunks

    Returns:
        Tuple of (content_hash, buffer, content_length)
    """
    hash_obj = hashlib.sha256()
    chunks: list[bytes] = []

    async for chunk in data:
        hash_obj.update(chunk)
        chunks.append(chunk)

    buffer = b"".join(chunks)
    return content_hash_from_digest(hash_obj.hexdigest()), buffer, len(buffer)


async def verify_async_iterator_hash(
    data: AsyncIterator[bytes], expected_hash: ContentHash[K]
) -> bool:
    """Verify that async iterator content matches an expected content hash.

    Args:
        data: Async iterator of bytes to verify
        expected_hash: Expected ContentHash

    Returns:
        True if hash matches
    """
    actual_hash: ContentHash[str]
    actual_hash, _, _ = await compute_hash_from_async_iterator(data)
    return str(actual_hash) == str(expected_hash)
