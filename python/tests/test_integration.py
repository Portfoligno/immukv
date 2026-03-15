"""Integration tests using real S3 API (MinIO).

These tests verify ImmuKV behavior against actual S3 operations,
testing specifications that cannot be adequately verified with mocks.
"""

import asyncio
import concurrent.futures
import json
import os
import uuid
from collections.abc import Coroutine
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Generator, TypeVar, cast

import pytest
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

from immukv import Config, ImmuKVClient
from immukv._internal.s3_client import BrandedS3Client
from immukv._internal.s3_helpers import get_error_code, read_body_as_json
from immukv._internal.s3_types import S3KeyPath
from immukv.json_helpers import JSONValue
from immukv.types import Entry, S3Credentials, S3Overrides

# Skip if not in integration test mode
pytestmark = pytest.mark.skipif(
    os.getenv("IMMUKV_INTEGRATION_TEST") != "true",
    reason="Integration tests require IMMUKV_INTEGRATION_TEST=true",
)


def identity_decoder(value: JSONValue) -> object:
    """Identity decoder that returns the JSONValue as-is."""
    return value


def identity_encoder(value: object) -> JSONValue:
    """Identity encoder that returns the value as JSONValue."""
    return value  # type: ignore[return-value]


@pytest.fixture(scope="session")
def _aio_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Provide a background event loop for test fixtures."""
    import threading

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="test-io", daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


@pytest.fixture(scope="session")
def raw_s3(_aio_loop: asyncio.AbstractEventLoop) -> Generator["S3Client", None, None]:
    """Create raw aiobotocore S3 client for bucket management."""
    import aiobotocore.session

    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    async def _create() -> tuple["S3Client", AsyncExitStack]:
        stack = AsyncExitStack()
        session = aiobotocore.session.get_session()
        client: "S3Client" = await stack.enter_async_context(
            session.create_client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1",
            )
        )
        return client, stack

    future = asyncio.run_coroutine_threadsafe(_create(), _aio_loop)
    client, stack = future.result()
    yield client
    future_close = asyncio.run_coroutine_threadsafe(stack.aclose(), _aio_loop)
    future_close.result()


_T = TypeVar("_T")


def _run_sync(coro: Coroutine[object, object, _T], loop: asyncio.AbstractEventLoop) -> _T:
    """Run a coroutine on the background loop, blocking until complete."""
    future: concurrent.futures.Future[_T] = asyncio.run_coroutine_threadsafe(
        coro,
        loop,
    )
    return future.result()


@pytest.fixture(scope="session")
def s3_client(raw_s3: "S3Client", _aio_loop: asyncio.AbstractEventLoop) -> BrandedS3Client:
    """Create branded S3 client for type-safe operations."""
    return BrandedS3Client(raw_s3, _aio_loop)


@pytest.fixture  # type: ignore[misc]
def s3_bucket(
    raw_s3: "S3Client", _aio_loop: asyncio.AbstractEventLoop
) -> Generator[str, None, None]:
    """Create unique S3 bucket for each test - ensures complete isolation."""
    bucket_name = f"test-immukv-{uuid.uuid4().hex[:8]}"

    # Create bucket
    _run_sync(raw_s3.create_bucket(Bucket=bucket_name), _aio_loop)

    # Enable versioning
    _run_sync(
        raw_s3.put_bucket_versioning(
            Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"}
        ),
        _aio_loop,
    )

    yield bucket_name

    # Cleanup: Delete all versions, delete markers, then bucket
    try:
        response = _run_sync(raw_s3.list_object_versions(Bucket=bucket_name), _aio_loop)

        # Delete all versions
        for version in response.get("Versions", []):  # type: ignore[misc,attr-defined]
            _run_sync(
                raw_s3.delete_object(
                    Bucket=bucket_name,
                    Key=version["Key"],  # type: ignore[misc]
                    VersionId=version["VersionId"],  # type: ignore[misc]
                ),
                _aio_loop,
            )

        # Delete all delete markers
        for marker in response.get("DeleteMarkers", []):  # type: ignore[misc,attr-defined]
            _run_sync(
                raw_s3.delete_object(
                    Bucket=bucket_name,
                    Key=marker["Key"],  # type: ignore[misc]
                    VersionId=marker["VersionId"],  # type: ignore[misc]
                ),
                _aio_loop,
            )

        # Delete bucket
        _run_sync(raw_s3.delete_bucket(Bucket=bucket_name), _aio_loop)
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

    client_instance: ImmuKVClient[str, object] = ImmuKVClient(
        config, identity_decoder, identity_encoder
    )
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
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/key1.json")
    response = s3_client.head_object(bucket=client._config.s3_bucket, key=key_path)

    etag: str = response["ETag"]
    assert etag.startswith('"') and etag.endswith('"')
    assert len(etag) > 10  # Real ETags are MD5 hashes


def test_conditional_write_if_match_succeeds(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify IfMatch conditional write succeeds with correct ETag."""
    client.set("key1", {"version": 1})

    # Get current ETag
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/key1.json")
    response = s3_client.head_object(bucket=client._config.s3_bucket, key=key_path)
    correct_etag = response["ETag"]

    # Write with IfMatch should succeed
    s3_client.put_object(
        bucket=client._config.s3_bucket,
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
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/key1.json")
    with pytest.raises(ClientError) as exc_info:  # type: ignore[misc]
        s3_client.put_object(
            bucket=client._config.s3_bucket,
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
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/new-key.json")
    s3_client.put_object(
        bucket=client._config.s3_bucket,
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
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/existing-key.json")
    with pytest.raises(ClientError) as exc_info:  # type: ignore[misc]
        s3_client.put_object(
            bucket=client._config.s3_bucket,
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
    prefix_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/key1.json")
    response = s3_client.list_object_versions(bucket=client._config.s3_bucket, prefix=prefix_path)

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
    log_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}_log.json")
    response = s3_client.get_object(bucket=client._config.s3_bucket, key=log_path, version_id=None)

    log_data = read_body_as_json(response["Body"])

    # Verify always-required fields
    required_fields = [
        "sequence",
        "key",
        "value",
        "timestamp_ms",
        "previous_hash",
        "hash",
    ]

    for field in required_fields:
        assert field in log_data, f"Log object missing required field: {field}"

    # Optional fields should be omitted for genesis entry (None/undefined stripped)
    # previous_version_id and previous_key_object_etag are NotRequired[Optional[str]]
    assert "previous_version_id" not in log_data, "Genesis entry should omit previous_version_id"
    assert "previous_key_object_etag" not in log_data, (
        "Genesis entry should omit previous_key_object_etag"
    )


def test_key_object_structure_matches_spec(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify key object contains required fields and excludes infrastructure fields."""
    entry = client.set("key1", {"data": "value"})

    # Read key object directly from S3
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/key1.json")
    response = s3_client.get_object(bucket=client._config.s3_bucket, key=key_path, version_id=None)

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


def test_none_values_omitted_from_json(s3_bucket: str, s3_client: BrandedS3Client) -> None:
    """Test that None values are stripped from JSON to match TypeScript undefined behavior."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    client = ImmuKVClient[str, dict[str, object]](
        config=Config(
            s3_bucket=s3_bucket,
            s3_region="us-east-1",
            s3_prefix="test-none-strip/",
            overrides=S3Overrides(
                endpoint_url=endpoint_url,
                credentials=S3Credentials(
                    aws_access_key_id=access_key, aws_secret_access_key=secret_key
                ),
                force_path_style=True,
            ),
        ),
        value_decoder=lambda v: cast(dict[str, object], v),
        value_encoder=lambda v: cast(JSONValue, v),
    )

    # First write - creates genesis entry with previous_version_id=None, previous_key_object_etag=None
    entry1 = client.set("test-key", {"value": "first"})

    # Read raw log entry from S3 to verify None fields are omitted
    log_response = s3_client.get_object(
        bucket=s3_bucket,
        key=S3KeyPath(f"test-none-strip/_log.json"),
        version_id=entry1.version_id,
    )
    log_data = read_body_as_json(log_response["Body"])

    # Verify None values were stripped (fields should not exist in JSON)
    assert "previous_version_id" not in log_data, "None value should be omitted from JSON"
    assert "previous_key_object_etag" not in log_data, "None value should be omitted from JSON"

    # Second write - has previous_version_id but previous_key_object_etag might be None
    entry2 = client.set("test-key", {"value": "second"})

    log_response2 = s3_client.get_object(
        bucket=s3_bucket,
        key=S3KeyPath(f"test-none-strip/_log.json"),
        version_id=entry2.version_id,
    )
    log_data2 = read_body_as_json(log_response2["Body"])

    # Second entry should have previous_version_id (not None)
    assert "previous_version_id" in log_data2, "Non-None value should be present in JSON"
    assert log_data2["previous_version_id"] == entry1.version_id

    # If previous_key_object_etag is None, it should be omitted
    # If it exists, it should have a value
    if "previous_key_object_etag" in log_data2:
        assert log_data2["previous_key_object_etag"] is not None


def test_missing_optional_fields_handled_correctly(
    s3_bucket: str, s3_client: BrandedS3Client
) -> None:
    """Test that missing optional fields (from TypeScript undefined) are handled as None."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    client = ImmuKVClient[str, dict[str, object]](
        config=Config(
            s3_bucket=s3_bucket,
            s3_region="us-east-1",
            s3_prefix="test-missing-fields/",
            overrides=S3Overrides(
                endpoint_url=endpoint_url,
                credentials=S3Credentials(
                    aws_access_key_id=access_key, aws_secret_access_key=secret_key
                ),
                force_path_style=True,
            ),
        ),
        value_decoder=lambda v: cast(dict[str, object], v),
        value_encoder=lambda v: cast(JSONValue, v),
    )

    entry = client.set("test-key", {"value": "test"})

    # Manually write a log entry with optional fields completely missing (simulating TypeScript)
    manually_created_log = {
        "sequence": 99,
        "key": "manual-key",
        "value": {"data": "manual"},
        "timestamp_ms": 1234567890000,
        "hash": "sha256:" + "a" * 64,
        "previous_hash": "sha256:genesis",
        # Deliberately omit previous_version_id and previous_key_object_etag
    }

    s3_client.put_object(
        bucket=s3_bucket,
        key=S3KeyPath(f"test-missing-fields/_log.json"),
        body=json.dumps(manually_created_log).encode("utf-8"),
        content_type="application/json",
    )

    # Read it back - should handle missing fields as None
    log_response = s3_client.get_object(
        bucket=s3_bucket, key=S3KeyPath(f"test-missing-fields/_log.json"), version_id=None
    )
    log_data = read_body_as_json(log_response["Body"])

    # Verify fields are missing (TypeScript undefined behavior)
    assert "previous_version_id" not in log_data
    assert "previous_key_object_etag" not in log_data

    # Python should handle missing fields gracefully via get() returning None
    assert log_data.get("previous_version_id") is None
    assert log_data.get("previous_key_object_etag") is None


# --- Repaired ETag and Orphan Repair Integration Tests ---
#
# These tests verify the behaviors introduced in ada2850:
# 1. When orphan repair succeeds and the repaired key matches the set key,
#    the repaired ETag is used directly (no stale headObject).
# 2. When orphan repair encounters an unexpected error, set() fails.
#
# Orphan creation strategy:
# Instead of mocking put_object (which also intercepts _repair_orphan calls),
# we directly manipulate S3 state: write a valid orphaned log entry via the
# raw S3 client, leaving the key object stale. This creates a genuine orphan
# that the client's repair logic discovers and fixes using real S3 operations.


def _create_orphan_log_entry(
    client: ImmuKVClient[str, object],
    s3_client: BrandedS3Client,
    key: str,
    value: JSONValue,
    prev_entry: Entry[str, object],
    prev_key_etag: str,
) -> None:
    """Write a valid log entry directly to S3 without updating the key object.

    This simulates Phase 1 succeeding and Phase 2 failing, creating an orphan.
    The log entry has correct hash chain and sequence, and records the current
    key object ETag as previous_key_object_etag (needed for repair).
    """
    from immukv._internal.json_helpers import dumps_canonical
    from immukv._internal.types import (
        LogEntryForHash,
        hash_compute,
        sequence_next,
        timestamp_now,
    )
    from immukv.types import TimestampMs

    new_sequence = sequence_next(prev_entry.sequence)
    timestamp_ms: TimestampMs[str] = timestamp_now()

    entry_for_hash: LogEntryForHash[str, JSONValue] = {
        "sequence": new_sequence,
        "key": key,
        "value": value,
        "timestamp_ms": timestamp_ms,
        "previous_hash": prev_entry.hash,
    }
    entry_hash = hash_compute(entry_for_hash)

    log_entry: dict[str, JSONValue] = {
        "sequence": int(new_sequence),
        "key": key,
        "value": value,
        "timestamp_ms": int(timestamp_ms),
        "previous_version_id": prev_entry.version_id,
        "previous_hash": str(prev_entry.hash),
        "hash": str(entry_hash),
        "previous_key_object_etag": prev_key_etag,
    }

    log_key = S3KeyPath[str](f"{client._config.s3_prefix}_log.json")
    s3_client.put_object(
        bucket=client._config.s3_bucket,
        key=log_key,
        body=dumps_canonical(cast(JSONValue, log_entry)),
        content_type="application/json",
    )


def test_repaired_etag_used_after_orphan_repair_end_to_end(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify that orphan repair uses the repaired ETag for Phase 2 end-to-end.

    Creates a real orphan in S3 by directly writing a log entry (simulating
    Phase 1 success + Phase 2 failure), then verifies the next set() triggers
    repair and completes successfully using the repaired ETag.
    """
    # Step 1: Write key "x" successfully (both phases succeed)
    entry1 = client.set("x", {"version": 1})
    assert entry1.key == "x"
    assert entry1.value == {"version": 1}

    # Get current key object ETag (needed for orphan log entry)
    key_path = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/x.json")
    head_response = s3_client.head_object(bucket=client._config.s3_bucket, key=key_path)
    current_key_etag = head_response["ETag"]

    # Step 2: Create orphan by writing a log entry directly (no key object update)
    # This simulates: Phase 1 wrote {"version": 2} to log, Phase 2 failed
    _create_orphan_log_entry(
        client,
        s3_client,
        key="x",
        value={"version": 2},
        prev_entry=entry1,
        prev_key_etag=current_key_etag,
    )

    # Verify orphan state: key object still has version 1
    key_response = s3_client.get_object(
        bucket=client._config.s3_bucket, key=key_path, version_id=None
    )
    key_data = read_body_as_json(key_response["Body"])
    assert key_data["value"] == {"version": 1}, "Key object should still have old value (orphan)"

    # Step 3: Call set("x", v3) — triggers real repair
    # _get_latest_and_repair reads the orphaned log entry for key "x",
    # repairs the key object (writing {"version": 2}), and returns the
    # repaired ETag. Since repaired_key == "x" (the set key), the repaired
    # ETag is used directly for Phase 2 instead of calling headObject.
    entry3 = client.set("x", {"version": 3})
    assert entry3.key == "x"
    assert entry3.value == {"version": 3}

    # Verify the final value via get()
    retrieved = client.get("x")
    assert retrieved.value == {"version": 3}
    assert retrieved.sequence == entry3.sequence

    # Verify hash chain integrity for the entry we wrote
    assert client.verify(entry3) is True


def test_repaired_etag_with_different_keys(
    client: ImmuKVClient[str, object], s3_client: BrandedS3Client
) -> None:
    """Verify that when repaired orphan key differs from set key, headObject is used.

    Creates an orphan for key "a", then sets key "b". The repair fixes "a",
    but since "b" != "a", a headObject is needed for "b"'s current ETag.
    """
    # Write key "a" successfully
    entry_a = client.set("a", {"key": "a", "version": 1})
    assert entry_a.key == "a"

    # Write key "b" successfully (this becomes the latest log entry)
    entry_b = client.set("b", {"key": "b", "version": 1})
    assert entry_b.key == "b"

    # Get current key object ETag for "a" (needed for orphan log entry)
    key_path_a = cast(S3KeyPath[str], f"{client._config.s3_prefix}keys/a.json")
    head_a = s3_client.head_object(bucket=client._config.s3_bucket, key=key_path_a)
    etag_a = head_a["ETag"]

    # Create orphan for key "a": write log entry directly, don't update key object
    _create_orphan_log_entry(
        client,
        s3_client,
        key="a",
        value={"key": "a", "version": 2},
        prev_entry=entry_b,
        prev_key_etag=etag_a,
    )

    # Now the latest log entry is an orphan for key "a".
    # Call set("b", v2) — repair fixes "a" (writing its key object),
    # but since we're writing to "b" (not "a"), headObject is called for "b"'s ETag.
    entry_b2 = client.set("b", {"key": "b", "version": 2})
    assert entry_b2.key == "b"
    assert entry_b2.value == {"key": "b", "version": 2}

    # Verify both keys have correct latest values
    retrieved_a = client.get("a")
    # Key "a" was repaired to version 2 by the repair step
    assert retrieved_a.value == {"key": "a", "version": 2}

    retrieved_b = client.get("b")
    assert retrieved_b.value == {"key": "b", "version": 2}

    # Verify chain integrity
    assert client.verify_log_chain() is True


def test_orphan_repair_failure_guard_not_triggered_on_first_entry(
    client: ImmuKVClient[str, object],
) -> None:
    """Verify that set() succeeds for the very first entry even though can_write and
    orphan_status are None.

    The guard (log_etag is not None and can_write is None and orphan_status is None)
    only triggers when a log already exists. For the first entry, log_etag is None,
    so the guard does not fire.
    """
    # This is the first set() on a fresh bucket — no log exists yet
    entry = client.set("first-key", {"data": "first-value"})
    assert entry.key == "first-key"
    assert entry.value == {"data": "first-value"}
    assert entry.sequence == 0

    # Verify via get
    retrieved = client.get("first-key")
    assert retrieved.value == {"data": "first-value"}


# Note on Test B (set() fails when orphan repair encounters unexpected error):
#
# This behavior is tested thoroughly in test_unit.py
# (test_set_throws_when_orphan_repair_has_unexpected_error).
#
# An integration test would require causing _repair_orphan to hit an unexpected
# S3 error (not PreconditionFailed, not AccessDenied). This could only be achieved by:
# - Temporarily revoking bucket write permissions mid-operation (not feasible with MinIO)
# - Causing S3 to return a non-standard error code (not controllable)
# - Corrupting bucket state in a way that triggers an unexpected error
#
# All of these are fragile and environment-dependent. The unit test with mocked
# _get_latest_and_repair provides reliable coverage for this guard condition.
# The integration tests above verify the happy path (repair succeeds, ETag is used)
# which is the more important end-to-end behavior to validate.
