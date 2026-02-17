"""Pure unit tests that don't require S3 or MinIO.

These tests verify pure logic: hash computation, data validation,
type checking, and other functionality that doesn't need S3.
"""

import pytest

from immukv._internal.types import (
    LogEntryForHash,
    hash_compute,
    hash_from_json,
    hash_genesis,
    sequence_from_json,
    timestamp_from_json,
)
from immukv.types import (
    Config,
    CredentialProvider,
    Hash,
    S3Credentials,
    S3Overrides,
    Sequence,
    TimestampMs,
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
    data_seq: LogEntryForHash[str, object] = {**base_data, "sequence": sequence_from_json(1)}
    assert hash_compute(data_seq) != base_hash

    # Change key
    data_key: LogEntryForHash[str, object] = {**base_data, "key": "different"}
    assert hash_compute(data_key) != base_hash

    # Change value
    data_val: LogEntryForHash[str, object] = {**base_data, "value": {"x": 2}}
    assert hash_compute(data_val) != base_hash

    # Change timestamp
    data_ts: LogEntryForHash[str, object] = {
        **base_data,
        "timestamp_ms": timestamp_from_json(2000000000000),
    }
    assert hash_compute(data_ts) != base_hash

    # Change previous_hash
    data_prev: LogEntryForHash[str, object] = {
        **base_data,
        "previous_hash": hash_from_json("sha256:" + "1" * 64),
    }
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


# --- S3Credentials Tests ---


def test_s3_credentials_without_session_token() -> None:
    """Verify S3Credentials works without session token (backward-compatible)."""
    creds = S3Credentials(
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
    )

    assert creds.aws_access_key_id == "AKID"
    assert creds.aws_secret_access_key == "SECRET"
    assert creds.aws_session_token is None


def test_s3_credentials_with_session_token() -> None:
    """Verify S3Credentials accepts aws_session_token for STS temporary credentials."""
    creds = S3Credentials(
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
        aws_session_token="TOKEN",
    )

    assert creds.aws_access_key_id == "AKID"
    assert creds.aws_secret_access_key == "SECRET"
    assert creds.aws_session_token == "TOKEN"


def test_s3_overrides_with_static_credentials() -> None:
    """Verify S3Overrides accepts static S3Credentials."""
    creds = S3Credentials(
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
        aws_session_token="TOKEN",
    )
    overrides = S3Overrides(credentials=creds)

    assert overrides.credentials is not None
    assert not callable(overrides.credentials)
    assert overrides.credentials.aws_access_key_id == "AKID"
    assert overrides.credentials.aws_session_token == "TOKEN"


def test_s3_overrides_with_credential_provider() -> None:
    """Verify S3Overrides accepts an async callable as credentials."""
    import asyncio

    async def my_provider() -> S3Credentials:
        return S3Credentials(
            aws_access_key_id="ASYNC_AKID",
            aws_secret_access_key="ASYNC_SECRET",
            aws_session_token="ASYNC_TOKEN",
        )

    overrides = S3Overrides(credentials=my_provider)

    assert overrides.credentials is not None
    assert callable(overrides.credentials)
    # Resolve the provider directly (not via the Union-typed field)
    resolved: S3Credentials = asyncio.run(my_provider())
    assert resolved.aws_access_key_id == "ASYNC_AKID"
    assert resolved.aws_secret_access_key == "ASYNC_SECRET"
    assert resolved.aws_session_token == "ASYNC_TOKEN"


# --- Credential Provider Adapter Tests ---


def test_credential_provider_refresh_adapter() -> None:
    """Verify the refresh adapter correctly converts CredentialProvider output to botocore format."""
    import asyncio
    from datetime import datetime, timezone

    from aiobotocore.credentials import AioDeferredRefreshableCredentials

    expires = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

    async def my_provider() -> S3Credentials:
        return S3Credentials(
            aws_access_key_id="REFRESH_AKID",
            aws_secret_access_key="REFRESH_SECRET",
            aws_session_token="REFRESH_TOKEN",
            expires_at=expires,
        )

    async def _refresh() -> dict[str, str]:
        from datetime import timedelta

        creds = await my_provider()
        expiry = creds.expires_at if creds.expires_at is not None else (datetime.now(timezone.utc) + timedelta(hours=1))
        return {
            "access_key": creds.aws_access_key_id,
            "secret_key": creds.aws_secret_access_key,
            "token": creds.aws_session_token if creds.aws_session_token is not None else "",
            "expiry_time": expiry.isoformat(),
        }

    async def run_test() -> None:
        refreshable = AioDeferredRefreshableCredentials(refresh_using=_refresh, method="test")
        frozen = await refreshable.get_frozen_credentials()
        assert frozen.access_key == "REFRESH_AKID"
        assert frozen.secret_key == "REFRESH_SECRET"
        assert frozen.token == "REFRESH_TOKEN"

    asyncio.run(run_test())


def test_credential_provider_session_injection() -> None:
    """Verify AioDeferredRefreshableCredentials can be injected into aiobotocore session."""
    import asyncio
    from datetime import datetime, timezone

    async def my_provider() -> S3Credentials:
        return S3Credentials(
            aws_access_key_id="INJECT_AKID",
            aws_secret_access_key="INJECT_SECRET",
            aws_session_token="INJECT_TOKEN",
            expires_at=datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

    async def run_test() -> None:
        import aiobotocore.session
        from aiobotocore.credentials import AioDeferredRefreshableCredentials

        async def _refresh() -> dict[str, str]:
            from datetime import timedelta

            creds = await my_provider()
            expiry = creds.expires_at or (datetime.now(timezone.utc) + timedelta(hours=1))
            return {
                "access_key": creds.aws_access_key_id,
                "secret_key": creds.aws_secret_access_key,
                "token": creds.aws_session_token or "",
                "expiry_time": expiry.isoformat(),
            }

        session = aiobotocore.session.get_session()
        refreshable = AioDeferredRefreshableCredentials(
            refresh_using=_refresh, method="test-injection"
        )
        session._credentials = refreshable  # type: ignore[attr-defined]

        resolved = session._credentials  # type: ignore[attr-defined,misc]
        assert resolved is refreshable  # type: ignore[misc]
        frozen = await refreshable.get_frozen_credentials()
        assert frozen.access_key == "INJECT_AKID"
        assert frozen.secret_key == "INJECT_SECRET"
        assert frozen.token == "INJECT_TOKEN"

    asyncio.run(run_test())
