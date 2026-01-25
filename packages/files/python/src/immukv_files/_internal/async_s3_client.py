"""Async S3 client wrapper for file operations."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, AsyncIterator, Generic, Literal, Optional, Tuple, TypeVar

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AioS3Client

from immukv_files.types import ContentHash, FileS3Key, FileVersionId
from immukv_files._internal.types import content_hash_from_digest
from immukv_files._internal.s3_helpers import (
    FileDeleteObjectOutput,
    FileDeleteObjectOutputs,
    FileGetObjectOutput,
    FileGetObjectOutputs,
    FilePutObjectOutput,
    FilePutObjectOutputs,
    GetBucketVersioningOutput,
    GetBucketVersioningOutputs,
    HeadBucketOutput,
    HeadBucketOutputs,
)

K = TypeVar("K", bound=str)
DEFAULT_CHUNK_SIZE = 64 * 1024


class AsyncFileS3Client(Generic[K]):
    """Async S3 client wrapper using shared aiobotocore client.

    Receives the aiobotocore client from ImmuKVClient.get_s3_client()
    to share the HTTP connection pool.
    """

    def __init__(self, aio_s3_client: "AioS3Client") -> None:
        self._s3 = aio_s3_client

    async def put_object_with_hash(
        self,
        bucket: str,
        key: FileS3Key[K],
        body: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        sse_kms_key_id: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
    ) -> Tuple[FilePutObjectOutput[K], ContentHash[K]]:
        """Upload bytes to S3, computing SHA-256 hash.

        Returns:
            Tuple of (FilePutObjectOutput with version_id/etag, content hash)
        """
        content_hash: ContentHash[K] = content_hash_from_digest(hashlib.sha256(body).hexdigest())

        request: dict[str, object] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type or "application/octet-stream",
        }
        if metadata is not None:
            request["Metadata"] = metadata
        if sse_kms_key_id is not None:
            request["SSEKMSKeyId"] = sse_kms_key_id
        if server_side_encryption is not None:
            request["ServerSideEncryption"] = server_side_encryption

        response = await self._s3.put_object(**request)  # type: ignore[arg-type]
        return FilePutObjectOutputs.from_aiobotocore(response), content_hash

    async def get_object(
        self,
        bucket: str,
        key: FileS3Key[K],
        version_id: Optional[FileVersionId[K]] = None,
    ) -> FileGetObjectOutput[K]:
        """Get object from S3.

        Returns FileGetObjectOutput with 'body' as async iterator.
        """
        request: dict[str, object] = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = await self._s3.get_object(**request)  # type: ignore[arg-type,misc]

        async def body_iterator() -> AsyncIterator[bytes]:
            async with response["Body"] as stream:  # type: ignore[misc]
                async for chunk, _ in stream.iter_chunks():  # type: ignore[misc]
                    yield chunk  # type: ignore[misc]

        return FileGetObjectOutputs.from_aiobotocore(response, body_iterator())  # type: ignore[misc]

    async def get_object_with_hash(
        self,
        bucket: str,
        key: FileS3Key[K],
        version_id: Optional[FileVersionId[K]] = None,
    ) -> Tuple[bytes, ContentHash[K]]:
        """Download object and compute hash.

        Returns:
            Tuple of (content bytes, content hash)
        """
        request: dict[str, object] = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = await self._s3.get_object(**request)  # type: ignore[arg-type,misc]

        hash_obj = hashlib.sha256()
        chunks: list[bytes] = []

        async with response["Body"] as stream:  # type: ignore[misc]
            async for chunk, _ in stream.iter_chunks():  # type: ignore[misc]
                hash_obj.update(chunk)  # type: ignore[arg-type,misc]
                chunks.append(chunk)  # type: ignore[arg-type,misc]

        content = b"".join(chunks)
        return content, content_hash_from_digest(hash_obj.hexdigest())

    async def delete_object(
        self,
        bucket: str,
        key: FileS3Key[K],
    ) -> FileDeleteObjectOutput[K]:
        """Delete object from S3.

        Returns FileDeleteObjectOutput with delete marker's version_id.
        """
        response = await self._s3.delete_object(Bucket=bucket, Key=key)
        return FileDeleteObjectOutputs.from_aiobotocore(response)

    async def get_bucket_versioning(self, bucket: str) -> GetBucketVersioningOutput:
        """Get bucket versioning status."""
        response = await self._s3.get_bucket_versioning(Bucket=bucket)
        return GetBucketVersioningOutputs.from_aiobotocore(response)

    async def head_bucket(self, bucket: str) -> HeadBucketOutput:
        """Check bucket access."""
        response = await self._s3.head_bucket(Bucket=bucket)
        return HeadBucketOutputs.from_aiobotocore(response)
