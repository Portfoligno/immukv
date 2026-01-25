"""Async S3 client wrapper for type-safe operations using aiobotocore.

This client is not part of the public API and should only be used internally.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Generic, Literal, Optional, TypeVar

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AioS3Client

from aiobotocore.session import AioSession, get_session
from botocore.config import Config as BotocoreConfig

from immukv._internal.s3_types import (
    GetObjectOutput,
    GetObjectOutputs,
    HeadObjectOutput,
    HeadObjectOutputs,
    ListObjectVersionsOutput,
    ListObjectVersionsOutputs,
    ListObjectsV2Output,
    ListObjectsV2Outputs,
    PutObjectOutput,
    PutObjectOutputs,
)
from immukv.types import Config

K = TypeVar("K", bound=str)


class AsyncBrandedS3Client(Generic[K]):
    """Async S3 client wrapper returning branded types.

    Mirrors the sync BrandedS3Client but uses aiobotocore.
    Centralizes all casts from aiobotocore's Any types to our
    clean Any-free type definitions.

    Features:
    - All S3 operations are async
    - Branded output types (no Any leakage)
    - Proper response body cleanup
    """

    def __init__(self, s3_client: "AioS3Client") -> None:
        """Initialize with an aiobotocore S3 client."""
        self._s3: "AioS3Client" = s3_client
        self._closed: bool = False

    async def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
    ) -> GetObjectOutput[K]:
        """Get object from S3.

        Note: Body is read immediately for small JSON objects.
        """
        request: dict[str, object] = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = await self._s3.get_object(**request)  # type: ignore[arg-type]

        # Read body immediately for small JSON objects
        body = response.get("Body")
        content: bytes = b""
        if body is not None:
            async with body as stream:
                content = await stream.read()

        return GetObjectOutputs.from_aiobotocore(response, content)

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> PutObjectOutput[K]:
        """Put object to S3 with optional conditional writes."""
        request: dict[str, object] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type or "application/json",
        }
        if if_match is not None:
            request["IfMatch"] = if_match
        if if_none_match is not None:
            request["IfNoneMatch"] = if_none_match
        if server_side_encryption is not None:
            request["ServerSideEncryption"] = server_side_encryption
        if sse_kms_key_id is not None:
            request["SSEKMSKeyId"] = sse_kms_key_id

        response = await self._s3.put_object(**request)  # type: ignore[arg-type]
        return PutObjectOutputs.from_aiobotocore(response)

    async def head_object(
        self,
        bucket: str,
        key: str,
    ) -> HeadObjectOutput[K]:
        """Get object metadata without downloading content."""
        response = await self._s3.head_object(Bucket=bucket, Key=key)
        return HeadObjectOutputs.from_aiobotocore(response)

    async def list_object_versions(
        self,
        bucket: str,
        prefix: str,
        key_marker: Optional[str] = None,
        version_id_marker: Optional[str] = None,
        max_keys: Optional[int] = None,
    ) -> ListObjectVersionsOutput[K]:
        """List object versions for pagination through history."""
        request: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if key_marker is not None:
            request["KeyMarker"] = key_marker
        if version_id_marker is not None:
            request["VersionIdMarker"] = version_id_marker
        if max_keys is not None:
            request["MaxKeys"] = max_keys

        response = await self._s3.list_object_versions(**request)  # type: ignore[arg-type]
        return ListObjectVersionsOutputs.from_aiobotocore(response)

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str,
        start_after: Optional[str] = None,
        continuation_token: Optional[str] = None,
        max_keys: Optional[int] = None,
    ) -> ListObjectsV2Output:
        """List objects for key enumeration."""
        request: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if start_after is not None:
            request["StartAfter"] = start_after
        if continuation_token is not None:
            request["ContinuationToken"] = continuation_token
        if max_keys is not None:
            request["MaxKeys"] = max_keys

        response = await self._s3.list_objects_v2(**request)  # type: ignore[arg-type]
        return ListObjectsV2Outputs.from_aiobotocore(response)

    async def close(self) -> None:
        """Mark client as closed (actual cleanup handled by context manager)."""
        self._closed = True


@asynccontextmanager
async def create_async_s3_client(
    config: Config,
) -> AsyncIterator[AsyncBrandedS3Client[str]]:
    """Factory to create async S3 client with managed lifecycle.

    Args:
        config: ImmuKV configuration with S3 settings

    Yields:
        AsyncBrandedS3Client ready for use

    Example:
        async with create_async_s3_client(config) as s3:
            response = await s3.get_object(bucket, key)
    """
    session: AioSession = get_session()

    client_kwargs: dict[str, object] = {
        "region_name": config.s3_region,
    }

    if config.overrides is not None:
        if config.overrides.endpoint_url is not None:
            client_kwargs["endpoint_url"] = config.overrides.endpoint_url
        if config.overrides.credentials is not None:
            client_kwargs["aws_access_key_id"] = config.overrides.credentials.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = (
                config.overrides.credentials.aws_secret_access_key
            )
        if config.overrides.force_path_style:
            client_kwargs["config"] = BotocoreConfig(s3={"addressing_style": "path"})

    async with session.create_client("s3", **client_kwargs) as client:  # type: ignore[arg-type,call-overload,misc]
        yield AsyncBrandedS3Client(client)  # type: ignore[misc]
