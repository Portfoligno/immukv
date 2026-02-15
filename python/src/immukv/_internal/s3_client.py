"""Branded S3 client wrapper for type-safe operations (aiobotocore backend).

This client is not part of the public API and should only be used internally.
"""

import asyncio
from collections.abc import Coroutine
from io import BytesIO
from typing import TYPE_CHECKING, Literal, Optional, TypeVar

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import (
        GetObjectRequestTypeDef,
        ListObjectVersionsRequestTypeDef,
        PutObjectRequestTypeDef,
    )

from immukv._internal.s3_types import (
    GetObjectOutput,
    HeadObjectOutput,
    HeadObjectOutputs,
    ListObjectVersionsOutput,
    ListObjectVersionsOutputs,
    ListObjectsV2Output,
    Object,
    PutObjectOutput,
    PutObjectOutputs,
    S3KeyPath,
    assert_aws_field_present,
)

K = TypeVar("K", bound=str)
_T = TypeVar("_T")


class BrandedS3Client:
    """Branded S3 client wrapper returning nominally-typed responses.

    Wraps an aiobotocore S3 client running on a background event loop.
    Each method calls the async aiobotocore operation via
    run_coroutine_threadsafe + future.result(), returning synchronous
    results with the same branded types as the previous boto3 version.
    """

    def __init__(self, s3_client: "S3Client", loop: asyncio.AbstractEventLoop) -> None:
        self._s3 = s3_client
        self._loop = loop

    def _run(self, coro: Coroutine[object, object, _T]) -> _T:
        """Bridge: submit coroutine to background loop, block for result."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    # -- GetObject ----------------------------------------------------------

    def get_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
        version_id: Optional[str] = None,
    ) -> GetObjectOutput[K]:
        """Get object from S3 (synchronous)."""
        return self._run(self._async_get_object(bucket, key, version_id))

    async def _async_get_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
        version_id: Optional[str] = None,
    ) -> GetObjectOutput[K]:
        request: "GetObjectRequestTypeDef" = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = await self._s3.get_object(**request)

        # Read the async streaming body NOW, before returning.
        # aiobotocore Body is async -- we read it fully and wrap in BytesIO
        # so that downstream read_body_as_json(response["Body"]) works unchanged.
        raw_body = await response["Body"].read()

        return {
            "Body": BytesIO(raw_body),
            "ETag": assert_aws_field_present(response.get("ETag"), "GetObjectOutput.ETag"),
            "VersionId": response.get("VersionId"),
        }

    # -- PutObject ----------------------------------------------------------

    def put_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> PutObjectOutput[K]:
        """Put object to S3 (synchronous)."""
        return self._run(
            self._async_put_object(
                bucket,
                key,
                body,
                content_type,
                if_match,
                if_none_match,
                server_side_encryption,
                sse_kms_key_id,
            )
        )

    async def _async_put_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> PutObjectOutput[K]:
        request: "PutObjectRequestTypeDef" = {"Bucket": bucket, "Key": key, "Body": body}
        if content_type is not None:
            request["ContentType"] = content_type
        if if_match is not None:
            request["IfMatch"] = if_match
        if if_none_match is not None:
            request["IfNoneMatch"] = if_none_match
        if server_side_encryption is not None:
            request["ServerSideEncryption"] = server_side_encryption
        if sse_kms_key_id is not None:
            request["SSEKMSKeyId"] = sse_kms_key_id

        response = await self._s3.put_object(**request)
        return PutObjectOutputs.from_aiobotocore(response)

    # -- HeadObject ---------------------------------------------------------

    def head_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
    ) -> HeadObjectOutput[K]:
        """Get object metadata from S3 (synchronous)."""
        return self._run(self._async_head_object(bucket, key))

    async def _async_head_object(
        self,
        bucket: str,
        key: S3KeyPath[K],
    ) -> HeadObjectOutput[K]:
        return HeadObjectOutputs.from_aiobotocore(
            await self._s3.head_object(Bucket=bucket, Key=key)
        )

    # -- ListObjectVersions -------------------------------------------------

    def list_object_versions(
        self,
        bucket: str,
        prefix: S3KeyPath[K],
        key_marker: Optional[S3KeyPath[K]] = None,
        version_id_marker: Optional[str] = None,
    ) -> ListObjectVersionsOutput[K]:
        """List object versions (synchronous)."""
        return self._run(
            self._async_list_object_versions(bucket, prefix, key_marker, version_id_marker)
        )

    async def _async_list_object_versions(
        self,
        bucket: str,
        prefix: S3KeyPath[K],
        key_marker: Optional[S3KeyPath[K]] = None,
        version_id_marker: Optional[str] = None,
    ) -> ListObjectVersionsOutput[K]:
        request: "ListObjectVersionsRequestTypeDef" = {"Bucket": bucket, "Prefix": prefix}
        if key_marker is not None:
            request["KeyMarker"] = key_marker
        if version_id_marker is not None:
            request["VersionIdMarker"] = version_id_marker

        response = await self._s3.list_object_versions(**request)
        return ListObjectVersionsOutputs.from_aiobotocore(response)

    # -- ListObjectsV2 (replaces get_paginator) -----------------------------

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str,
        start_after: Optional[str] = None,
        continuation_token: Optional[str] = None,
    ) -> ListObjectsV2Output:
        """List objects (single page, synchronous).

        Replaces get_paginator("list_objects_v2"). The caller drives
        pagination by passing back NextContinuationToken.
        """
        return self._run(
            self._async_list_objects_v2(bucket, prefix, start_after, continuation_token)
        )

    async def _async_list_objects_v2(
        self,
        bucket: str,
        prefix: str,
        start_after: Optional[str] = None,
        continuation_token: Optional[str] = None,
    ) -> ListObjectsV2Output:
        if continuation_token is not None:
            response = await self._s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token
            )
        elif start_after is not None:
            response = await self._s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, StartAfter=start_after
            )
        else:
            response = await self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        contents_raw = response.get("Contents")
        contents: Optional[list[Object]] = None
        if contents_raw is not None:
            contents = [
                {"Key": assert_aws_field_present(obj.get("Key"), "Object.Key")}
                for obj in contents_raw
            ]

        return {
            "Contents": contents,
            "IsTruncated": assert_aws_field_present(
                response.get("IsTruncated"), "ListObjectsV2Output.IsTruncated"
            ),
            "NextContinuationToken": response.get("NextContinuationToken"),
        }
