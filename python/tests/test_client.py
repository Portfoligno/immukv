"""Integration tests for ImmuKV client using MinIO.

These tests require MinIO running and test actual S3 operations.
Run with: IMMUKV_INTEGRATION_TEST=true IMMUKV_S3_ENDPOINT=http://localhost:9000 pytest
"""

import os
import uuid
from typing import Generator

import boto3
import pytest
from mypy_boto3_s3.client import S3Client

from immukv import Config, ImmuKVClient
from immukv._internal.s3_client import BrandedS3Client
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
    """Create raw S3 client for bucket management operations."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://localhost:4566")
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
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://localhost:9000")
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


def test_set_and_get_single_entry(client: ImmuKVClient[str, object]) -> None:
    """Test basic set and get operations."""
    # Set a value
    entry = client.set("key1", {"data": "value1"})

    # Verify entry structure
    assert entry.key == "key1"
    assert entry.value == {"data": "value1"}
    assert entry.sequence == 0
    assert entry.previous_hash == "sha256:genesis"
    assert entry.hash.startswith("sha256:")
    assert entry.version_id is not None
    assert entry.previous_version_id is None

    # Get the value back
    retrieved = client.get("key1")

    assert retrieved.key == entry.key
    assert retrieved.value == entry.value
    assert retrieved.sequence == entry.sequence
    assert retrieved.hash == entry.hash


def test_set_multiple_entries_same_key(client: ImmuKVClient[str, object]) -> None:
    """Test multiple writes to the same key."""
    # Write three values to the same key
    entry1 = client.set("sensor-01", {"temp": 20.5})
    entry2 = client.set("sensor-01", {"temp": 21.0})
    entry3 = client.set("sensor-01", {"temp": 19.8})

    # Verify sequence numbers are contiguous
    assert entry1.sequence == 0
    assert entry2.sequence == 1
    assert entry3.sequence == 2

    # Verify hash chain
    assert entry1.previous_hash == "sha256:genesis"
    assert entry2.previous_hash == entry1.hash
    assert entry3.previous_hash == entry2.hash

    # Get should return latest value
    latest = client.get("sensor-01")
    assert latest.value == {"temp": 19.8}
    assert latest.sequence == 2


def test_set_multiple_keys(client: ImmuKVClient[str, object]) -> None:
    """Test writing to multiple different keys."""
    entry1 = client.set("key-a", {"value": "a"})
    entry2 = client.set("key-b", {"value": "b"})
    entry3 = client.set("key-c", {"value": "c"})

    # All entries should be in the log with contiguous sequences
    assert entry1.sequence == 0
    assert entry2.sequence == 1
    assert entry3.sequence == 2

    # Each key should retrieve its own value
    assert client.get("key-a").value == {"value": "a"}
    assert client.get("key-b").value == {"value": "b"}
    assert client.get("key-c").value == {"value": "c"}


def test_history_single_key(client: ImmuKVClient[str, object]) -> None:
    """Test retrieving history for a single key."""
    # Write multiple versions
    client.set("metric", {"count": 1})
    client.set("metric", {"count": 2})
    client.set("metric", {"count": 3})

    # Get full history
    entries, oldest_version = client.history("metric", None, None)

    # Should return all 3 entries in descending order (newest first)
    assert len(entries) == 3
    assert entries[0].value == {"count": 3}
    assert entries[1].value == {"count": 2}
    assert entries[2].value == {"count": 1}

    # Verify sequences
    assert entries[0].sequence == 2
    assert entries[1].sequence == 1
    assert entries[2].sequence == 0


def test_history_with_limit(client: ImmuKVClient[str, object]) -> None:
    """Test history retrieval with limit."""
    # Write 5 versions
    for i in range(5):
        client.set("counter", {"value": i})

    # Get only first 3
    entries, oldest_version = client.history("counter", None, 3)

    assert len(entries) == 3
    assert entries[0].value == {"value": 4}  # Newest
    assert entries[1].value == {"value": 3}
    assert entries[2].value == {"value": 2}


def test_history_mixed_keys(client: ImmuKVClient[str, object]) -> None:
    """Test that history only returns entries for requested key."""
    # Mix writes to different keys
    client.set("key-x", {"data": "x1"})
    client.set("key-y", {"data": "y1"})
    client.set("key-x", {"data": "x2"})
    client.set("key-y", {"data": "y2"})
    client.set("key-x", {"data": "x3"})

    # Get history for key-x
    entries, _ = client.history("key-x", None, None)

    # Should only have 3 entries for key-x
    assert len(entries) == 3
    assert all(e.key == "key-x" for e in entries)
    assert entries[0].value == {"data": "x3"}
    assert entries[1].value == {"data": "x2"}
    assert entries[2].value == {"data": "x1"}


def test_log_entries(client: ImmuKVClient[str, object]) -> None:
    """Test retrieving entries from global log."""
    # Write to multiple keys
    client.set("k1", {"v": 1})
    client.set("k2", {"v": 2})
    client.set("k1", {"v": 3})

    # Get all log entries
    entries = client.log_entries(None, None)

    # Should have 3 entries in descending order (newest first)
    assert len(entries) == 3
    assert entries[0].key == "k1"
    assert entries[0].value == {"v": 3}
    assert entries[0].sequence == 2

    assert entries[1].key == "k2"
    assert entries[1].value == {"v": 2}
    assert entries[1].sequence == 1

    assert entries[2].key == "k1"
    assert entries[2].value == {"v": 1}
    assert entries[2].sequence == 0


def test_log_entries_with_limit(client: ImmuKVClient[str, object]) -> None:
    """Test log retrieval with limit."""
    # Write 5 entries
    for i in range(5):
        client.set(f"key-{i}", {"index": i})

    # Get only 3 newest
    entries = client.log_entries(None, 3)

    assert len(entries) == 3
    assert entries[0].sequence == 4
    assert entries[1].sequence == 3
    assert entries[2].sequence == 2


def test_list_keys(client: ImmuKVClient[str, object]) -> None:
    """Test listing all keys."""
    # Write to multiple keys (not in alphabetical order)
    client.set("zebra", {"animal": "z"})
    client.set("apple", {"fruit": "a"})
    client.set("banana", {"fruit": "b"})

    # List all keys
    keys = client.list_keys(None, None)

    # Should return in lexicographic order
    assert len(keys) == 3
    assert keys == ["apple", "banana", "zebra"]


def test_list_keys_with_pagination(client: ImmuKVClient[str, object]) -> None:
    """Test key listing with pagination."""
    # Create several keys
    for i in range(5):
        client.set(f"key-{i:02d}", {"index": i})

    # Get first 3 keys
    keys = client.list_keys(None, 3)
    assert len(keys) == 3
    assert keys == ["key-00", "key-01", "key-02"]

    # Get next batch after "key-01"
    keys = client.list_keys("key-01", 2)
    assert len(keys) == 2
    assert keys == ["key-02", "key-03"]


def test_verify_single_entry(client: ImmuKVClient[str, object]) -> None:
    """Test verifying a single entry's hash."""
    entry = client.set("test-key", {"field": "value"})

    # Verify should pass for valid entry
    assert client.verify(entry) is True


def test_verify_corrupted_entry(client: ImmuKVClient[str, object]) -> None:
    """Test that verification fails for corrupted entry."""
    entry = client.set("test-key", {"field": "value"})

    # Corrupt the entry
    entry.value["field"] = "corrupted"  # type: ignore[index]

    # Verification should fail
    assert client.verify(entry) is False


def test_verify_log_chain(client: ImmuKVClient[str, object]) -> None:
    """Test verifying the entire log hash chain."""
    # Write several entries
    client.set("k1", {"v": 1})
    client.set("k2", {"v": 2})
    client.set("k3", {"v": 3})

    # Verify entire chain
    assert client.verify_log_chain() is True


def test_verify_log_chain_with_limit(client: ImmuKVClient[str, object]) -> None:
    """Test verifying only recent entries in chain."""
    # Write several entries
    for i in range(10):
        client.set(f"key-{i}", {"index": i})

    # Verify only last 5 entries
    assert client.verify_log_chain(limit=5) is True


def test_get_nonexistent_key(client: ImmuKVClient[str, object]) -> None:
    """Test getting a key that doesn't exist."""
    from immukv.client import KeyNotFoundError

    # Write one key
    client.set("existing-key", {"data": "value"})

    # Try to get non-existent key
    with pytest.raises(KeyNotFoundError):
        client.get("nonexistent-key")


def test_history_nonexistent_key(client: ImmuKVClient[str, object]) -> None:
    """Test history for a key that was never written."""
    # Write to other keys
    client.set("other-key", {"data": "value"})

    # History for non-existent key should return empty list
    entries, oldest_version = client.history("nonexistent-key", None, None)
    assert entries == []
    assert oldest_version is None


def test_hash_chain_integrity(client: ImmuKVClient[str, object]) -> None:
    """Test that hash chain links entries correctly."""
    entry1 = client.set("chain-test", {"step": 1})
    entry2 = client.set("chain-test", {"step": 2})
    entry3 = client.set("chain-test", {"step": 3})

    # Verify chain links
    assert entry1.previous_hash == "sha256:genesis"
    assert entry2.previous_hash == entry1.hash
    assert entry3.previous_hash == entry2.hash

    # Each hash should be unique
    assert entry1.hash != entry2.hash
    assert entry2.hash != entry3.hash
    assert entry1.hash != entry3.hash


def test_sequence_numbers_contiguous(client: ImmuKVClient[str, object]) -> None:
    """Test that sequence numbers are contiguous across different keys."""
    entries = []

    # Mix writes to different keys
    entries.append(client.set("a", {"v": 1}))
    entries.append(client.set("b", {"v": 2}))
    entries.append(client.set("a", {"v": 3}))
    entries.append(client.set("c", {"v": 4}))
    entries.append(client.set("b", {"v": 5}))

    # Verify all sequences are contiguous
    for i, entry in enumerate(entries):
        assert entry.sequence == i


def test_get_log_version(client: ImmuKVClient[str, object]) -> None:
    """Test retrieving a specific log entry by version id."""
    entry1 = client.set("versioned", {"data": "first"})
    entry2 = client.set("versioned", {"data": "second"})

    # Retrieve first entry by its version id
    retrieved = client.get_log_version(entry1.version_id)

    assert retrieved.sequence == entry1.sequence
    assert retrieved.value == {"data": "first"}
    assert retrieved.hash == entry1.hash


def test_read_only_mode(client: ImmuKVClient[str, object]) -> None:
    """Test that read-only mode works correctly."""
    # First write some data with normal client
    client.set("readonly-test", {"value": "data"})

    # Create a read-only client
    config = Config(
        s3_bucket=client.config.s3_bucket,
        s3_region=client.config.s3_region,
        s3_prefix=client.config.s3_prefix,
        read_only=True,
        overrides=client.config.overrides,
    )

    ro_client: ImmuKVClient[str, object] = ImmuKVClient(config, identity_parser)
    with ro_client:
        # Read should work
        entry = ro_client.get("readonly-test")
        assert entry.value == {"value": "data"}

        # Write should fail (implementation detail - may raise or handle differently)
        # For now, just verify read works


def test_custom_endpoint_url_config(s3_bucket: str) -> None:
    """Test that overrides can be specified for S3-compatible services."""
    # Config with overrides should be accepted
    config = Config(
        s3_bucket=s3_bucket,
        s3_region="us-east-1",
        s3_prefix="test/",
        overrides=S3Overrides(endpoint_url="http://localhost:4566"),
    )

    # Client creation should succeed
    client_instance: ImmuKVClient[str, object] = ImmuKVClient(config, identity_parser)

    # Verify overrides are stored in config
    assert client_instance.config.overrides is not None
    assert client_instance.config.overrides.endpoint_url == "http://localhost:4566"

    # Note: Actual operations would require MinIO/moto running at that endpoint.
    # This test verifies the config accepts and stores the overrides correctly.


def test_default_overrides_is_none(s3_bucket: str) -> None:
    """Test that overrides defaults to None for AWS S3."""
    # Config without overrides specified
    config = Config(
        s3_bucket=s3_bucket,
        s3_region="us-east-1",
        s3_prefix="test/",
    )

    client_instance: ImmuKVClient[str, object] = ImmuKVClient(config, identity_parser)

    # Should default to None (uses AWS S3)
    assert client_instance.config.overrides is None
