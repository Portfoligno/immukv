"""Branded S3 client wrapper for type-safe operations.

This client is not part of the public API and should only be used internally.
"""

from typing import Literal, Optional, TypeVar, cast, overload

from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.paginator import ListObjectsV2Paginator
from mypy_boto3_s3.type_defs import (
    GetObjectRequestTypeDef,
    ListObjectVersionsRequestTypeDef,
    PutObjectRequestTypeDef,
)

from immukv._internal.s3_types import (
    LogKey,
    S3GetObjectResponse,
    S3HeadObjectResponse,
    S3KeyPath,
    S3ListObjectVersionsPage,
    S3PutObjectResponse,
)

K = TypeVar("K", bound=str)


class BrandedS3Client:
    """Branded S3 client wrapper returning nominally-typed responses.

    Centralizes all casts from boto3's Any-containing types to our
    clean Any-free type definitions. This allows strict mypy checking
    (disallow_any_expr) while working with boto3.
    """

    def __init__(self, s3_client: S3Client) -> None:
        """Initialize with a boto3 S3 client."""
        self._s3 = s3_client

    @overload
    def get_object(
        self,
        bucket: str,
        key: S3KeyPath[LogKey],
        version_id: Optional[str] = None,
    ) -> "S3GetObjectResponse[LogKey]": ...

    @overload
    def get_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
        version_id: Optional[str] = None,
    ) -> "S3GetObjectResponse[K]": ...

    def get_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
        version_id: Optional[str] = None,
    ) -> "S3GetObjectResponse[K]":
        """Get object from S3."""
        request: GetObjectRequestTypeDef = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            request["VersionId"] = version_id

        response = self._s3.get_object(**request)
        return cast("S3GetObjectResponse[K]", response)

    @overload
    def put_object(
        self,
        bucket: str,
        key: S3KeyPath[LogKey],
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "S3PutObjectResponse[LogKey]": ...

    @overload
    def put_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "S3PutObjectResponse[K]": ...

    def put_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        server_side_encryption: Optional[Literal["AES256", "aws:kms", "aws:kms:dsse"]] = None,
        sse_kms_key_id: Optional[str] = None,
    ) -> "S3PutObjectResponse[K]":
        """Put object to S3."""
        request: PutObjectRequestTypeDef = {"Bucket": bucket, "Key": key, "Body": body}
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

        response = self._s3.put_object(**request)
        return cast("S3PutObjectResponse[K]", response)

    @overload
    def head_object(
        self,
        bucket: str,
        key: S3KeyPath[LogKey],
    ) -> "S3HeadObjectResponse[LogKey]": ...

    @overload
    def head_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
    ) -> "S3HeadObjectResponse[K]": ...

    def head_object(
        self,
        bucket: str,
        key: "S3KeyPath[K]",
    ) -> "S3HeadObjectResponse[K]":
        """Get object metadata from S3."""
        return cast("S3HeadObjectResponse[K]", self._s3.head_object(Bucket=bucket, Key=key))

    @overload
    def list_object_versions(
        self,
        bucket: str,
        prefix: S3KeyPath[LogKey],
        key_marker: Optional[S3KeyPath[LogKey]] = None,
        version_id_marker: Optional[str] = None,
    ) -> "S3ListObjectVersionsPage[LogKey]": ...

    @overload
    def list_object_versions(
        self,
        bucket: str,
        prefix: "S3KeyPath[K]",
        key_marker: "Optional[S3KeyPath[K]]" = None,
        version_id_marker: Optional[str] = None,
    ) -> "S3ListObjectVersionsPage[K]": ...

    def list_object_versions(
        self,
        bucket: str,
        prefix: "S3KeyPath[K]",
        key_marker: "Optional[S3KeyPath[K]]" = None,
        version_id_marker: Optional[str] = None,
    ) -> "S3ListObjectVersionsPage[K]":
        """List object versions."""
        request: ListObjectVersionsRequestTypeDef = {"Bucket": bucket, "Prefix": prefix}
        if key_marker is not None:
            request["KeyMarker"] = key_marker
        if version_id_marker is not None:
            request["VersionIdMarker"] = version_id_marker

        response = self._s3.list_object_versions(**request)
        return cast("S3ListObjectVersionsPage[K]", response)

    def get_paginator(self, operation_name: Literal["list_objects_v2"]) -> ListObjectsV2Paginator:
        """Get paginator for list operations."""
        return self._s3.get_paginator(operation_name)
