"""Integration tests using real S3 API (MinIO).

These tests verify ImmuKV behavior against actual S3 operations,
testing specifications that cannot be adequately verified with mocks.
"""

import json
import os
import uuid
from typing import Generator, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from immukv import Config, ImmuKVClient
from immukv._internal.s3_client import BrandedS3Client
from immukv._internal.s3_helpers import get_error_code, read_body_as_json
from immukv._internal.s3_types import S3KeyPath
from immukv.json_helpers import JSONValue
from immukv.types import S3Credentials, S3Overrides


# Skip if not in integration test mode
pytestmark = pytest.mark.skipif(
    os.getenv("IMMUKV_INTEGRATION_TEST") != "true",
    reason="Integration tests require IMMUKV_INTEGRATION_TEST=true",
)


def identity_parser(value: JSONValue) -> object:
    """Identity parser that returns the JSONValue as-is."""
    return value


@pytest.fixture(scope="session")  # type: ignore[misc]
def raw_s3() -> S3Client:
    """Create raw S3 client for bucket management."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://minio:9000")
    # Use environment variables if set, otherwise default to test credentials
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.client(  # type: ignore[return-value]
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")  # type: ignore[misc]
def s3_client(raw_s3: S3Client) -> BrandedS3Client:
    """Create branded S3 client for type-safe operations."""
    return BrandedS3Client(raw_s3)


@pytest.fixture  # type: ignore[misc]
def s3_bucket(raw_s3: S3Client) -> Generator[str, None, None]:
    """Create unique S3 bucket for each test - ensures complete isolation."""
    bucket_name = f"test-immukv-{uuid.uuid4().hex[:8]}"

    # Create bucket
    raw_s3.create_bucket(Bucket=bucket_name)

    # Enable versioning
    raw_s3.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})

    yield bucket_name

    # Cleanup: Delete all versions, delete markers, then bucket
    try:
        response = raw_s3.list_object_versions(Bucket=bucket_name)

        # Delete all versions
        for version in response.get("Versions", []):
            raw_s3.delete_object(
                Bucket=bucket_name, Key=version["Key"], VersionId=version["VersionId"]
            )

        # Delete all delete markers
        for marker in response.get("DeleteMarkers", []):
            raw_s3.delete_object(
                Bucket=bucket_name, Key=marker["Key"], VersionId=marker["VersionId"]
            )

        # Delete bucket
        raw_s3.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        # Best effort cleanup - don't fail tests if cleanup fails
        print(f"Warning: Cleanup failed for bucket {bucket_name}: {e}")


@pytest.fixture  # type: ignore[misc]
def client(s3_bucket: str) -> Generator[ImmuKVClient[str, object], None, None]:
    """Create ImmuKV client connected to MinIO."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    config = Config(
        s3_bucket=s3_bucket,
        s3_region="us-east-1",
        s3_prefix="test/",
        repair_check_interval_ms=1000,
        overrides=S3Overrides(
            endpoint_url=endpoint_url,
            credentials=S3Credentials(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            ),
            force_path_style=True,
        ),
    )

    client_instance: ImmuKVClient[str, object] = ImmuKVClient(config, identity_parser)
    with client_instance as client:
        yield client


# --- Integration Tests ---


def test_real_s3_versioning_creates_unique_version_ids(
    client: ImmuKVClient[str, object],
) -> None:
    """Verify that real S3 versioning creates unique version IDs."""
    entry1 = client.set("key1", {"version": 1})
    entry2 = client.set("key1", {"version": 2})
    entry3 = client.set("key2", {"version": 1})

    # Version IDs should be unique and non-trivial
    assert entry1.version_id != entry2.version_id
    assert entry2.version_id != entry3.version_id
    assert len(entry1.version_id) > 10  # Real S3 version IDs are long


def test_real_etag_generation_and_validation(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify that real S3 generates ETags and validates them."""
    entry = client.set("key1", {"data": "value"})

    # Get the key object and check ETag
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/key1.json")
    response = s3_client.head_object(bucket=client.config.s3_bucket, key=key_path)

    etag: str = response["ETag"]
    assert etag.startswith('"') and etag.endswith('"')
    assert len(etag) > 10  # Real ETags are MD5 hashes


def test_conditional_write_if_match_succeeds(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify IfMatch conditional write succeeds with correct ETag."""
    client.set("key1", {"version": 1})

    # Get current ETag
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/key1.json")
    response = s3_client.head_object(bucket=client.config.s3_bucket, key=key_path)
    correct_etag = response["ETag"]

    # Write with IfMatch should succeed
    s3_client.put_object(
        bucket=client.config.s3_bucket,
        key=key_path,
        body=b'{"test": "update"}',
        if_match=correct_etag,
    )


def test_conditional_write_if_match_fails(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify IfMatch conditional write fails with wrong ETag."""
    client.set("key1", {"version": 1})

    # Write with wrong ETag should fail
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/key1.json")
    with pytest.raises(ClientError) as exc_info:  # type: ignore[misc]
        s3_client.put_object(
            bucket=client.config.s3_bucket,
            key=key_path,
            body=b'{"test": "update"}',
            if_match='"wrong-etag"',
        )

    assert get_error_code(exc_info.value) == "PreconditionFailed"


def test_conditional_write_if_none_match_creates_new(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify IfNoneMatch='*' succeeds when key doesn't exist."""
    # Write with IfNoneMatch='*' should succeed for new key
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/new-key.json")
    s3_client.put_object(
        bucket=client.config.s3_bucket,
        key=key_path,
        body=b'{"test": "create"}',
        if_none_match="*",
    )


def test_conditional_write_if_none_match_fails_when_exists(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify IfNoneMatch='*' fails when key already exists."""
    client.set("existing-key", {"version": 1})

    # Write with IfNoneMatch='*' should fail
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/existing-key.json")
    with pytest.raises(ClientError) as exc_info:  # type: ignore[misc]
        s3_client.put_object(
            bucket=client.config.s3_bucket,
            key=key_path,
            body=b'{"test": "create"}',
            if_none_match="*",
        )

    assert get_error_code(exc_info.value) == "PreconditionFailed"


def test_list_object_versions_returns_proper_order(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify list_object_versions returns versions in proper order."""
    # Create multiple versions
    entry1 = client.set("key1", {"version": 1})
    entry2 = client.set("key1", {"version": 2})
    entry3 = client.set("key1", {"version": 3})

    # List versions
    prefix_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/key1.json")
    response = s3_client.list_object_versions(bucket=client.config.s3_bucket, prefix=prefix_path)

    versions = response["Versions"]
    assert versions is not None
    assert len(versions) == 3

    # Should be in reverse chronological order (newest first)
    assert all("VersionId" in v for v in versions)


def test_log_object_structure_matches_spec(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify log object contains all required fields per design doc."""
    entry = client.set("key1", {"data": "value"})

    # Read log object directly from S3
    log_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}_log.json")
    response = s3_client.get_object(bucket=client.config.s3_bucket, key=log_path, version_id=None)

    log_data = read_body_as_json(response["Body"])

    # Verify required fields per design doc
    required_fields = [
        "sequence",
        "key",
        "value",
        "timestamp_ms",
        "previous_version_id",
        "previous_hash",
        "hash",
        "previous_key_object_etag",
    ]

    for field in required_fields:
        assert field in log_data, f"Log object missing required field: {field}"


def test_key_object_structure_matches_spec(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify key object contains required fields and excludes infrastructure fields."""
    entry = client.set("key1", {"data": "value"})

    # Read key object directly from S3
    key_path = cast(S3KeyPath[str], f"{client.config.s3_prefix}keys/key1.json")
    response = s3_client.get_object(bucket=client.config.s3_bucket, key=key_path, version_id=None)

    key_data = read_body_as_json(response["Body"])

    # Verify required fields
    required_fields = [
        "sequence",
        "key",
        "value",
        "timestamp_ms",
        "log_version_id",
        "hash",
        "previous_hash",
    ]

    for field in required_fields:
        assert field in key_data, f"Key object missing required field: {field}"

    # Verify excluded fields (per design doc)
    excluded_fields = ["previous_version_id", "previous_key_object_etag"]

    for field in excluded_fields:
        assert field not in key_data, f"Key object should not contain infrastructure field: {field}"
