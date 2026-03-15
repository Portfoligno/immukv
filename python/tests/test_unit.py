"""Pure unit tests that don't require S3 or MinIO.

These tests verify pure logic: hash computation, data validation,
type checking, and other functionality that doesn't need S3.
"""

from typing import TYPE_CHECKING

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

    expires = datetime(2030, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

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
        if creds.expires_at is not None:
            expiry = creds.expires_at
        else:
            expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        token = creds.aws_session_token if creds.aws_session_token is not None else ""
        return {
            "access_key": creds.aws_access_key_id,
            "secret_key": creds.aws_secret_access_key,
            "token": token,
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
            expires_at=datetime(2030, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

    async def run_test() -> None:
        import aiobotocore.session
        from aiobotocore.credentials import AioDeferredRefreshableCredentials

        async def _refresh() -> dict[str, str]:
            from datetime import timedelta

            creds = await my_provider()
            if creds.expires_at is not None:
                expiry = creds.expires_at
            else:
                expiry = datetime.now(timezone.utc) + timedelta(hours=1)
            token = creds.aws_session_token if creds.aws_session_token is not None else ""
            return {
                "access_key": creds.aws_access_key_id,
                "secret_key": creds.aws_secret_access_key,
                "token": token,
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


# --- Repaired ETag and orphan repair failure tests ---
#
# These tests mock _get_latest_and_repair() and verify set() behaviour
# around orphan repair, repaired ETags, and error handling.
# They use a real ImmuKVClient with a fully mocked BrandedS3Client
# so no S3/MinIO is needed.

if TYPE_CHECKING:
    from immukv import ImmuKVClient


def _make_mock_client() -> "ImmuKVClient[str, object]":
    """Create an ImmuKVClient with a fully mocked S3 backend for unit testing."""
    from typing import cast
    from unittest.mock import MagicMock

    from immukv import Config, ImmuKVClient
    from immukv._internal.s3_client import BrandedS3Client
    from immukv._internal.s3_types import S3KeyPaths
    from immukv.json_helpers import JSONValue

    config = Config(
        s3_bucket="unit-test-bucket",
        s3_region="us-east-1",
        s3_prefix="test/",
    )

    def identity_decoder(value: JSONValue) -> object:
        return value

    def identity_encoder(value: object) -> JSONValue:
        return value  # type: ignore[return-value]

    # Build the client by bypassing __init__ (which starts threads and connects to S3)
    client: ImmuKVClient[str, object] = object.__new__(ImmuKVClient)
    client._config = config
    client._value_decoder = identity_decoder  # type: ignore[assignment]
    client._value_encoder = identity_encoder  # type: ignore[assignment]
    client._s3 = MagicMock(spec=BrandedS3Client)  # type: ignore[assignment]
    client._log_key = S3KeyPaths.for_log(config.s3_prefix)  # type: ignore[assignment]
    client._last_repair_check_ms = 0
    client._can_write = None
    client._latest_orphan_status = None
    return client


def test_repaired_etag_used_when_orphan_key_matches_set_key() -> None:
    """Test that headObject is skipped when repaired orphan key matches the set key."""
    from unittest.mock import patch

    from immukv._internal.types import LatestLogState, hash_from_json, sequence_from_json

    client = _make_mock_client()

    repaired_etag = '"repaired-etag-123"'
    mock_result: LatestLogState[str] = {
        "log_etag": '"some-log-etag"',
        "prev_version_id": "prev-version-1",  # type: ignore[typeddict-item]
        "prev_hash": hash_from_json("sha256:" + "a" * 64),
        "sequence": sequence_from_json(0),
        "can_write": True,
        "orphan_status": {
            "is_orphaned": False,
            "orphan_key": "target-key",
            "orphan_entry": None,
            "checked_at": 0,
        },
        "repaired_key": "target-key",
        "repaired_key_object_etag": repaired_etag,  # type: ignore[typeddict-item]
    }

    # Mock put_object to return a valid response
    client._s3.put_object.return_value = {  # type: ignore[attr-defined,misc]
        "ETag": '"new-etag"',
        "VersionId": "new-version-id",
    }

    with patch.object(client, "_get_latest_and_repair", return_value=mock_result):
        entry = client.set("target-key", {"data": "updated"})

        # head_object should NOT have been called because the repaired ETag was used
        client._s3.head_object.assert_not_called()  # type: ignore[attr-defined]

        # The set should succeed
        assert entry.key == "target-key"
        assert entry.value == {"data": "updated"}


def test_headobject_fallback_when_orphan_key_differs_from_set_key() -> None:
    """Test that headObject IS called when repaired orphan key differs from set key."""
    from unittest.mock import patch

    from immukv._internal.types import LatestLogState, hash_from_json, sequence_from_json

    client = _make_mock_client()

    repaired_etag = '"repaired-etag-456"'
    mock_result: LatestLogState[str] = {
        "log_etag": '"some-log-etag"',
        "prev_version_id": "prev-version-1",  # type: ignore[typeddict-item]
        "prev_hash": hash_from_json("sha256:" + "b" * 64),
        "sequence": sequence_from_json(1),
        "can_write": True,
        "orphan_status": {
            "is_orphaned": False,
            "orphan_key": "orphan-key",
            "orphan_entry": None,
            "checked_at": 0,
        },
        "repaired_key": "orphan-key",
        "repaired_key_object_etag": repaired_etag,  # type: ignore[typeddict-item]
    }

    # Mock head_object to return a valid response for the different key
    client._s3.head_object.return_value = {  # type: ignore[attr-defined,misc]
        "ETag": '"existing-key-etag"',
        "VersionId": "existing-version-id",
    }

    # Mock put_object to return a valid response
    client._s3.put_object.return_value = {  # type: ignore[attr-defined,misc]
        "ETag": '"new-etag"',
        "VersionId": "new-version-id",
    }

    with patch.object(client, "_get_latest_and_repair", return_value=mock_result):
        entry = client.set("other-key", {"data": "updated"})

        # head_object SHOULD have been called because the repaired key != set key
        client._s3.head_object.assert_called()  # type: ignore[attr-defined]

        # The set should succeed
        assert entry.key == "other-key"
        assert entry.value == {"data": "updated"}


def test_set_throws_when_orphan_repair_has_unexpected_error() -> None:
    """Test that set() fails when orphan repair returns unexpected error."""
    from unittest.mock import patch

    from immukv._internal.types import LatestLogState, hash_from_json, sequence_from_json

    client = _make_mock_client()

    # Mock _get_latest_and_repair to simulate: _repair_orphan returned (None, None, None)
    # This means can_write=None, orphan_status=None, and log_etag is defined
    mock_result: LatestLogState[str] = {
        "log_etag": '"some-log-etag"',
        "prev_version_id": "prev-version-1",  # type: ignore[typeddict-item]
        "prev_hash": hash_from_json("sha256:" + "c" * 64),
        "sequence": sequence_from_json(0),
        "can_write": None,
        "orphan_status": None,
        "repaired_key": None,
        "repaired_key_object_etag": None,
    }

    with patch.object(client, "_get_latest_and_repair", return_value=mock_result):
        # set() should raise because orphan repair failed with unexpected error
        with pytest.raises(
            RuntimeError,
            match="Cannot proceed with set\\(\\): orphan repair failed with unexpected error",
        ):
            client.set("some-key", {"data": "new-value"})


def test_set_does_not_throw_when_log_etag_is_none() -> None:
    """Test that set() succeeds when logEtag is None even if canWrite and orphanStatus are None."""
    from unittest.mock import patch

    from immukv._internal.types import LatestLogState, hash_genesis, sequence_initial

    client = _make_mock_client()

    # Mock _get_latest_and_repair to simulate: no log exists yet (first entry)
    # can_write=None and orphan_status=None, but log_etag is also None
    # This should NOT throw because there's no log to have an orphan
    mock_result: LatestLogState[str] = {
        "log_etag": None,
        "prev_version_id": None,  # type: ignore[typeddict-item]
        "prev_hash": hash_genesis(),
        "sequence": sequence_initial(),
        "can_write": None,
        "orphan_status": None,
        "repaired_key": None,
        "repaired_key_object_etag": None,
    }

    # Mock put_object to return a valid response
    client._s3.put_object.return_value = {  # type: ignore[attr-defined,misc]
        "ETag": '"first-etag"',
        "VersionId": "first-version-id",
    }

    with patch.object(client, "_get_latest_and_repair", return_value=mock_result):
        # Should succeed because log_etag is None (guard: log_etag is not None)
        entry = client.set("first-key", {"data": "first-value"})
        assert entry.key == "first-key"
        assert entry.value == {"data": "first-value"}
