"""Pure unit tests that don't require S3 or MinIO.

These tests verify pure logic: hash computation, data validation,
type checking, and other functionality that doesn't need S3.
"""

import pytest

from immukv.types import (
    Config,
    Hash,
    LogEntryForHash,
    S3Overrides,
    Sequence,
    TimestampMs,
    hash_compute,
    hash_genesis,
    hash_from_json,
    sequence_from_json,
    timestamp_from_json,
)


# --- Hash Computation Tests ---


def test_hash_compute_format() -> None:
    """Verify hash_compute returns 'sha256:' prefix with 64 hex characters."""
    data: LogEntryForHash[str, object] = {
        "sequence": sequence_from_json(0),
        "key": "test-key",
        "value": {"field": "value"},
        "timestamp_ms": timestamp_from_json(1234567890000),
        "previous_hash": hash_from_json("sha256:genesis"),
    }

    result = hash_compute(data)

    # Must start with 'sha256:'
    assert result.startswith("sha256:")

    # Must be exactly 71 characters total (sha256: + 64 hex)
    assert len(result) == 71

    # Hex portion must be exactly 64 characters
    hex_part = result[7:]  # After 'sha256:'
    assert len(hex_part) == 64
    assert all(c in "0123456789abcdef" for c in hex_part)


def test_hash_compute_deterministic() -> None:
    """Verify hash_compute produces same hash for same input."""
    data: LogEntryForHash[str, object] = {
        "sequence": sequence_from_json(5),
        "key": "key1",
        "value": {"a": 1, "b": 2},
        "timestamp_ms": timestamp_from_json(1000000000000),
        "previous_hash": hash_from_json("sha256:abcd" + "0" * 60),
    }

    hash1 = hash_compute(data)
    hash2 = hash_compute(data)

    assert hash1 == hash2


def test_hash_compute_changes_with_different_data() -> None:
    """Verify hash changes when any field changes."""
    base_data: LogEntryForHash[str, object] = {
        "sequence": sequence_from_json(0),
        "key": "key",
        "value": {"x": 1},
        "timestamp_ms": timestamp_from_json(1000000000000),
        "previous_hash": hash_from_json("sha256:genesis"),
    }

    base_hash = hash_compute(base_data)

    # Change sequence
    data_seq: LogEntryForHash[str, object] = {**base_data, "sequence": sequence_from_json(1)}  # type: ignore[misc]
    assert hash_compute(data_seq) != base_hash

    # Change key
    data_key: LogEntryForHash[str, object] = {**base_data, "key": "different"}  # type: ignore[misc]
    assert hash_compute(data_key) != base_hash

    # Change value
    data_val: LogEntryForHash[str, object] = {**base_data, "value": {"x": 2}}  # type: ignore[misc]
    assert hash_compute(data_val) != base_hash

    # Change timestamp
    data_ts: LogEntryForHash[str, object] = {**base_data, "timestamp_ms": timestamp_from_json(2000000000000)}  # type: ignore[misc]
    assert hash_compute(data_ts) != base_hash

    # Change previous_hash
    data_prev: LogEntryForHash[str, object] = {**base_data, "previous_hash": hash_from_json("sha256:" + "1" * 64)}  # type: ignore[misc]
    assert hash_compute(data_prev) != base_hash


def test_hash_genesis() -> None:
    """Verify hash_genesis returns the correct genesis hash."""
    genesis: Hash[str] = hash_genesis()

    assert genesis == "sha256:genesis"
    assert isinstance(genesis, Hash)


def test_hash_from_json_valid() -> None:
    """Verify hash_from_json accepts valid hash strings."""
    valid_hash = "sha256:" + "a" * 64
    result: Hash[str] = hash_from_json(valid_hash)

    assert result == valid_hash
    assert isinstance(result, Hash)


def test_hash_from_json_genesis() -> None:
    """Verify hash_from_json accepts genesis hash."""
    result: Hash[str] = hash_from_json("sha256:genesis")

    assert result == "sha256:genesis"


def test_hash_from_json_invalid_prefix() -> None:
    """Verify hash_from_json rejects invalid prefix."""
    with pytest.raises(ValueError, match="must start with 'sha256:'"):
        hash_from_json("md5:" + "a" * 64)


# Note: hash_from_json only validates prefix, not hex length/format
# Actual hash validation happens during hash computation and comparison


# --- Timestamp Validation Tests ---


def test_timestamp_from_json_valid() -> None:
    """Verify timestamp_from_json accepts valid epoch milliseconds."""
    # Year 2024
    ts: TimestampMs[str] = timestamp_from_json(1700000000000)

    assert ts == 1700000000000
    assert isinstance(ts, TimestampMs)


def test_timestamp_from_json_accepts_large_values() -> None:
    """Verify timestamp_from_json accepts typical epoch millisecond values."""
    # Typical: 1000000000000+ (year 2001+)
    ts: TimestampMs[str] = timestamp_from_json(1700000000000)
    assert ts == 1700000000000


def test_timestamp_from_json_zero() -> None:
    """Verify timestamp_from_json rejects zero."""
    with pytest.raises(ValueError, match="must be > 0"):
        timestamp_from_json(0)


def test_timestamp_from_json_negative() -> None:
    """Verify timestamp_from_json rejects negative values."""
    with pytest.raises(ValueError, match="must be > 0"):
        timestamp_from_json(-1)


# --- Config Validation Tests ---


def test_config_required_fields() -> None:
    """Verify Config requires s3_bucket, s3_region, s3_prefix."""
    config = Config(
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        s3_prefix="test/",
    )

    assert config.s3_bucket == "test-bucket"
    assert config.s3_region == "us-east-1"
    assert config.s3_prefix == "test/"


def test_config_optional_fields_defaults() -> None:
    """Verify Config optional fields have correct defaults."""
    config = Config(
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        s3_prefix="test/",
    )

    assert config.kms_key_id is None
    assert config.overrides is None
    assert config.repair_check_interval_ms == 300000  # 5 minutes
    assert config.read_only is False


def test_config_with_all_optional_fields() -> None:
    """Verify Config accepts all optional fields."""
    config = Config(
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        s3_prefix="test/",
        kms_key_id="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
        repair_check_interval_ms=60000,
        read_only=True,
        overrides=S3Overrides(endpoint_url="http://localhost:4566"),
    )

    assert config.kms_key_id is not None
    assert config.overrides is not None
    assert config.overrides.endpoint_url == "http://localhost:4566"
    assert config.repair_check_interval_ms == 60000
    assert config.read_only is True
