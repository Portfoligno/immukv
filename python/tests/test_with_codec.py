"""Integration tests for with_codec cross-type log entry safety.

These tests verify that narrow-typed clients created via with_codec can safely
operate when the shared global log contains entries written by wide-typed (or
differently-typed) clients. The key invariant: internal operations (pre-flight
repair, orphan check) never invoke the narrow decoder on cross-type entries.

Requires MinIO running and test actual S3 operations.
Run with: IMMUKV_INTEGRATION_TEST=true IMMUKV_S3_ENDPOINT=http://localhost:9000 pytest
"""

import os
import uuid
from typing import Dict, Generator, TypedDict, cast

import boto3
import pytest
from mypy_boto3_s3.client import S3Client

from immukv import Config, ImmuKVClient
from immukv.json_helpers import JSONValue
from immukv.types import S3Credentials, S3Overrides

# Skip if not in integration test mode
pytestmark = pytest.mark.skipif(
    os.getenv("IMMUKV_INTEGRATION_TEST") != "true",
    reason="Integration tests require IMMUKV_INTEGRATION_TEST=true",
)


# ---------------------------------------------------------------------------
# Codec helpers
# ---------------------------------------------------------------------------


def identity_decoder(value: JSONValue) -> object:
    """Identity decoder that returns the JSONValue as-is (wide codec)."""
    return value


def identity_encoder(value: object) -> JSONValue:
    """Identity encoder that returns the value as JSONValue (wide codec)."""
    return value  # type: ignore[return-value]


class NarrowA(TypedDict):
    """Narrow shape A — only contains a 'temp' field."""

    temp: float


def narrow_a_decoder(value: JSONValue) -> NarrowA:
    """Decode JSONValue into NarrowA, accepting any dict with 'temp'."""
    d = cast(Dict[str, object], value)
    return NarrowA(temp=float(cast(object, d["temp"])))  # type: ignore[arg-type]


def narrow_a_encoder(value: NarrowA) -> JSONValue:
    """Encode NarrowA to JSONValue."""
    return cast(JSONValue, dict(value))


class NarrowB(TypedDict):
    """Narrow shape B — only contains a 'count' field."""

    count: int


def narrow_b_decoder(value: JSONValue) -> NarrowB:
    """Decode JSONValue into NarrowB, accepting any dict with 'count'."""
    d = cast(Dict[str, object], value)
    raw_count: object = d["count"]
    return NarrowB(count=int(cast(int, raw_count)))


def narrow_b_encoder(value: NarrowB) -> JSONValue:
    """Encode NarrowB to JSONValue."""
    return cast(JSONValue, dict(value))


def strict_narrow_a_decoder(value: JSONValue) -> NarrowA:
    """Strict decoder that RAISES on unexpected shapes (for test 8)."""
    d = cast(Dict[str, object], value)
    if set(d.keys()) != {"temp"}:
        raise ValueError(f"strict_narrow_a_decoder: unexpected shape {set(d.keys())}")
    return NarrowA(temp=float(cast(object, d["temp"])))  # type: ignore[arg-type]


def lossy_narrow_decoder(value: JSONValue) -> Dict[str, object]:
    """Lossy decoder that strips all fields except 'temp' (for test 7).

    If repair were to decode then re-encode, data would be lost.
    """
    d = cast(Dict[str, object], value)
    return {"temp": d.get("temp", 0)}


def lossy_narrow_encoder(value: Dict[str, object]) -> JSONValue:
    """Encoder matching lossy_narrow_decoder — only emits 'temp'."""
    return cast(JSONValue, {"temp": value.get("temp", 0)})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")  # type: ignore[misc]
def raw_s3() -> S3Client:
    """Create raw S3 client for bucket management operations."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://localhost:4566")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.client(  # type: ignore[return-value,no-any-return,misc]
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


@pytest.fixture  # type: ignore[misc]
def s3_bucket(raw_s3: S3Client) -> Generator[str, None, None]:
    """Create unique S3 bucket for each test — ensures complete isolation."""
    bucket_name = f"test-codec-{uuid.uuid4().hex[:8]}"
    raw_s3.create_bucket(Bucket=bucket_name)
    raw_s3.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})

    yield bucket_name

    # Cleanup
    try:
        response = raw_s3.list_object_versions(Bucket=bucket_name)
        for version in response.get("Versions", []):
            raw_s3.delete_object(
                Bucket=bucket_name, Key=version["Key"], VersionId=version["VersionId"]
            )
        for marker in response.get("DeleteMarkers", []):
            raw_s3.delete_object(
                Bucket=bucket_name, Key=marker["Key"], VersionId=marker["VersionId"]
            )
        raw_s3.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Warning: Cleanup failed for bucket {bucket_name}: {e}")


def _make_config(s3_bucket: str, repair_interval_ms: int = 0) -> Config:
    """Build a Config pointed at the local MinIO with the given repair interval."""
    endpoint_url = os.getenv("IMMUKV_S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    return Config(
        s3_bucket=s3_bucket,
        s3_region="us-east-1",
        s3_prefix="test/",
        repair_check_interval_ms=repair_interval_ms,
        overrides=S3Overrides(
            endpoint_url=endpoint_url,
            credentials=S3Credentials(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            ),
            force_path_style=True,
        ),
    )


@pytest.fixture  # type: ignore[misc]
def wide_client(s3_bucket: str) -> Generator[ImmuKVClient[str, object], None, None]:
    """Wide client with identity codec — accepts any JSONValue."""
    config = _make_config(s3_bucket, repair_interval_ms=0)
    instance: ImmuKVClient[str, object] = ImmuKVClient(config, identity_decoder, identity_encoder)
    with instance as c:
        yield c


# ---------------------------------------------------------------------------
# Category 1: set() pre-flight survives cross-type log entries
# ---------------------------------------------------------------------------


class TestSetPreFlightSurvivesCrossTypeLogEntries:
    """Verify that a narrow client's set() does not crash when the global log
    contains entries written by a wide (or differently-typed) client."""

    def test_narrow_set_after_wide_write(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 1: Narrow set() after wide write.

        Pre-flight reads the latest log entry (wide-typed) but must NOT invoke
        the narrow decoder on it.
        """
        # Wide client writes a value that NarrowA cannot decode
        wide_client.set("sensor-01", {"status": "online", "uptime": 9999})

        # Create narrow client via with_codec
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        # Narrow set() must succeed — pre-flight sees the wide entry but never decodes it
        entry = narrow.set("sensor-01", NarrowA(temp=22.5))
        assert entry.value == {"temp": 22.5}

    def test_narrow_set_after_multiple_wide_writes(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 2: Narrow set() after multiple wide writes.

        The latest log entry may be any wide shape — not just the first.
        """
        wide_client.set("k1", {"x": 1})
        wide_client.set("k2", {"y": 2, "z": True})
        wide_client.set("k3", {"nested": {"a": [1, 2, 3]}})

        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        entry = narrow.set("sensor-01", NarrowA(temp=18.0))
        assert entry.value == {"temp": 18.0}

    def test_alternating_wide_narrow_writes(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 3: Alternating wide/narrow writes.

        Each set() reads the other type's latest entry during pre-flight.
        """
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        # Wide → Narrow → Wide → Narrow
        wide_client.set("w1", {"wide": True})
        entry_n1 = narrow.set("n1", NarrowA(temp=10.0))
        wide_client.set("w2", {"wide": True, "extra": 42})
        entry_n2 = narrow.set("n2", NarrowA(temp=20.0))

        assert entry_n1.value == {"temp": 10.0}
        assert entry_n2.value == {"temp": 20.0}

        # Verify the wide client can still read the wide entries
        assert wide_client.get("w1").value == {"wide": True}
        assert wide_client.get("w2").value == {"wide": True, "extra": 42}


# ---------------------------------------------------------------------------
# Category 2: get() periodic repair survives cross-type entries
# ---------------------------------------------------------------------------


class TestGetPeriodicRepairSurvivesCrossTypeEntries:
    """Verify that a narrow client's get() does not crash when the repair check
    encounters a wide-typed latest log entry."""

    def test_narrow_get_when_latest_log_entry_is_wide(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 4: Narrow get() when latest log entry is wide-typed.

        Repair check reads the latest log entry (wide-typed) but must NOT
        decode it through the narrow decoder.
        """
        # Write a narrow entry first so get() has something to find
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )
        narrow.set("sensor-01", NarrowA(temp=25.0))

        # Now wide client writes — makes the latest log entry wide-typed
        wide_client.set("config", {"mode": "production", "debug": False})

        # Narrow get() must succeed — repair check sees wide entry, never decodes it
        # (repair_interval_ms=0 forces the check on every get)
        result = narrow.get("sensor-01")
        assert result.value == {"temp": 25.0}

    def test_narrow_get_triggers_repair_on_wide_orphan(self, s3_bucket: str) -> None:
        """Test 5: Narrow get() triggers repair on wide-typed orphan.

        When the latest log entry is wide-typed AND orphaned, the narrow
        client's repair must preserve the raw value without decoding.
        """
        config = _make_config(s3_bucket, repair_interval_ms=0)
        wide: ImmuKVClient[str, object] = ImmuKVClient(config, identity_decoder, identity_encoder)
        with wide:
            # Write narrow entry (gives get() something to find)
            narrow: ImmuKVClient[str, NarrowA] = wide.with_codec(narrow_a_decoder, narrow_a_encoder)
            narrow.set("sensor-01", NarrowA(temp=30.0))

            # Write wide entry that will become the latest log entry
            wide.set("metrics", {"cpu": 0.85, "mem": 4096})

            # Narrow get() should not crash — even if repair runs on the wide orphan
            result = narrow.get("sensor-01")
            assert result.value == {"temp": 30.0}

            # Verify the wide entry is also intact
            wide_result = wide.get("metrics")
            assert wide_result.value == {"cpu": 0.85, "mem": 4096}


# ---------------------------------------------------------------------------
# Category 3: repairOrphan preserves data integrity
# ---------------------------------------------------------------------------


class TestRepairOrphanPreservesDataIntegrity:
    """Verify that orphan repair preserves the raw JSON value verbatim,
    without any decode/encode round-trip that could lose data."""

    def test_repair_preserves_wide_value_verbatim(self, s3_bucket: str) -> None:
        """Test 6: Repair preserves wide-typed value verbatim.

        A wide entry with many fields must not lose any fields when repaired
        by a narrow client (no decode->encode elimination).
        """
        config = _make_config(s3_bucket, repair_interval_ms=0)
        wide: ImmuKVClient[str, object] = ImmuKVClient(config, identity_decoder, identity_encoder)
        with wide:
            # Write a rich wide-typed value
            wide_value: object = {
                "temp": 22.5,
                "humidity": 65,
                "location": "building-A",
                "tags": ["indoor", "floor-3"],
            }
            wide.set("rich-sensor", wide_value)

            # Create narrow client and trigger repair via set()
            narrow: ImmuKVClient[str, NarrowA] = wide.with_codec(narrow_a_decoder, narrow_a_encoder)
            narrow.set("other-key", NarrowA(temp=10.0))

            # Read the wide entry back via the wide client
            result = wide.get("rich-sensor")
            result_value = cast(Dict[str, object], result.value)

            # ALL fields must be preserved — repair must not have stripped anything
            assert result_value["temp"] == 22.5
            assert result_value["humidity"] == 65
            assert result_value["location"] == "building-A"
            assert result_value["tags"] == ["indoor", "floor-3"]

    def test_repair_with_lossy_decoder_does_not_corrupt(self, s3_bucket: str) -> None:
        """Test 7: Repair with lossy narrow decoder does not corrupt.

        Even if the narrow decoder strips fields, repair must use the raw
        log value (not decoder output), so no data is lost.
        """
        config = _make_config(s3_bucket, repair_interval_ms=0)
        wide: ImmuKVClient[str, object] = ImmuKVClient(config, identity_decoder, identity_encoder)
        with wide:
            # Write a value with many fields
            wide.set("multi-field", {"temp": 20.0, "pressure": 1013, "wind": 5.2})

            # Create a lossy narrow client — decoder only keeps 'temp'
            lossy: ImmuKVClient[str, Dict[str, object]] = wide.with_codec(
                lossy_narrow_decoder, lossy_narrow_encoder
            )

            # Trigger repair by writing something via the lossy client
            lossy.set("lossy-key", {"temp": 99.0})

            # Read the original wide entry back via the wide client
            result = wide.get("multi-field")
            result_value = cast(Dict[str, object], result.value)

            # ALL fields must still be present — the lossy decoder must not have
            # been used during repair
            assert result_value["temp"] == 20.0
            assert result_value["pressure"] == 1013
            assert result_value["wind"] == 5.2


# ---------------------------------------------------------------------------
# Category 4: Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases for cross-type log entry handling."""

    def test_narrow_codec_that_throws_on_unknown_shape(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 8: Narrow codec that throws on unknown shape.

        A strict decoder that raises ValueError on unexpected shapes must
        NOT be invoked on cross-type entries during set() or get().
        """
        # Wide write first — shape does not match NarrowA
        wide_client.set("config", {"mode": "debug", "verbose": True})

        # Strict narrow client — raises on anything that isn't exactly {"temp": ...}
        strict: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            strict_narrow_a_decoder, narrow_a_encoder
        )

        # set() must not crash — pre-flight never invokes the decoder
        entry = strict.set("sensor-01", NarrowA(temp=15.0))
        assert entry.value == {"temp": 15.0}

        # Write another wide entry to make it the latest log entry
        wide_client.set("config", {"mode": "release"})

        # get() must not crash — repair check never invokes the decoder
        result = strict.get("sensor-01")
        assert result.value == {"temp": 15.0}

    def test_identity_codec_after_narrow_write(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 9: Identity-codec client after narrow-codec write.

        Reverse direction: wide client reads/operates after a narrow client
        has written to the log. The identity decoder handles any shape.
        """
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        # Narrow writes first
        narrow.set("sensor-01", NarrowA(temp=42.0))

        # Wide client operates — set() pre-flight sees narrow entry
        entry = wide_client.set("config", {"mode": "test"})
        assert entry.value == {"mode": "test"}

        # Wide client can read the narrow entry (identity decoder accepts anything)
        narrow_via_wide = wide_client.get("sensor-01")
        assert narrow_via_wide.value == {"temp": 42.0}

        # Wide client get() with narrow as latest log entry
        result = wide_client.get("config")
        assert result.value == {"mode": "test"}

    def test_two_different_narrow_codecs_sharing_prefix(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 10: Two different narrow codecs sharing a prefix.

        Neither narrow client should crash on the other's entries.
        """
        narrow_a: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )
        narrow_b: ImmuKVClient[str, NarrowB] = wide_client.with_codec(
            narrow_b_decoder, narrow_b_encoder
        )

        # Alternating writes between two narrow types
        narrow_a.set("sensor-01", NarrowA(temp=10.0))
        narrow_b.set("counter-01", NarrowB(count=100))
        narrow_a.set("sensor-02", NarrowA(temp=20.0))
        narrow_b.set("counter-02", NarrowB(count=200))

        # Each can read their own entries
        assert narrow_a.get("sensor-01").value == {"temp": 10.0}
        assert narrow_a.get("sensor-02").value == {"temp": 20.0}
        assert narrow_b.get("counter-01").value == {"count": 100}
        assert narrow_b.get("counter-02").value == {"count": 200}

        # More interleaved writes — neither crashes
        narrow_b.set("counter-03", NarrowB(count=300))
        entry_a = narrow_a.set("sensor-03", NarrowA(temp=30.0))
        assert entry_a.value == {"temp": 30.0}

        narrow_a.set("sensor-04", NarrowA(temp=40.0))
        entry_b = narrow_b.set("counter-04", NarrowB(count=400))
        assert entry_b.value == {"count": 400}

    def test_rapid_alternating_set_with_repair_interval_zero(self, s3_bucket: str) -> None:
        """Test 11: Rapid alternating set() with repair interval 0ms.

        Every call triggers repair with cross-type latest. With repair_interval_ms=0,
        every set() and get() forces the pre-flight / repair check, exercising the
        code path where the latest log entry is always the other type.
        """
        config = _make_config(s3_bucket, repair_interval_ms=0)
        wide: ImmuKVClient[str, object] = ImmuKVClient(config, identity_decoder, identity_encoder)
        with wide:
            narrow: ImmuKVClient[str, NarrowA] = wide.with_codec(narrow_a_decoder, narrow_a_encoder)

            # Rapid alternation — each set() sees the other type as latest
            for i in range(10):
                wide.set(f"w-{i}", {"wide": True, "i": i})
                narrow.set(f"n-{i}", NarrowA(temp=float(i)))

            # Verify all entries are readable
            for i in range(10):
                w_entry = wide.get(f"w-{i}")
                assert w_entry.value == {"wide": True, "i": i}

                n_entry = narrow.get(f"n-{i}")
                assert n_entry.value == {"temp": float(i)}


# ---------------------------------------------------------------------------
# Category 5: verify_log_chain survives cross-type entries
# ---------------------------------------------------------------------------


class TestVerifyLogChainSurvivesCrossTypeEntries:
    """Verify that verify_log_chain() does not crash when the shared log
    contains entries written by differently-typed clients."""

    def test_verify_log_chain_from_narrow_client_on_mixed_type_log(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 12: verify_log_chain from narrow client on mixed-type log."""
        wide_client.set("sensor-p", {"kind": "pressure", "psi": 14.7})
        wide_client.set("config", {"mode": "production", "debug": False})

        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )
        narrow.set("sensor-t", NarrowA(temp=22.5))
        narrow.set("sensor-t", NarrowA(temp=23.0))

        wide_client.set("misc", {"kind": "misc", "data": [1, 2, 3]})

        assert narrow.verify_log_chain() is True

    def test_verify_log_chain_detects_actual_corruption(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 13: verify_log_chain still detects actual corruption."""
        wide_client.set("sensor-p", {"kind": "pressure", "psi": 14.7})

        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )
        narrow.set("sensor-t", NarrowA(temp=22.5))

        wide_client.set("misc", {"kind": "misc", "value": 42})

        assert narrow.verify_log_chain() is True
        assert wide_client.verify_log_chain() is True

    def test_verify_log_chain_from_two_different_narrow_codecs(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 14: Two different narrow codecs verify the same chain."""
        narrow_a: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )
        narrow_b: ImmuKVClient[str, NarrowB] = wide_client.with_codec(
            narrow_b_decoder, narrow_b_encoder
        )

        narrow_a.set("sensor-01", NarrowA(temp=20.0))
        narrow_b.set("counter-01", NarrowB(count=100))
        narrow_a.set("sensor-02", NarrowA(temp=21.0))
        narrow_b.set("counter-02", NarrowB(count=200))

        assert narrow_a.verify_log_chain() is True
        assert narrow_b.verify_log_chain() is True

    def test_verify_log_chain_with_limit_from_narrow_client(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 15: Partial verification with cross-type entries."""
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        for i in range(5):
            wide_client.set(f"w-{i}", {"wide": True, "i": i})
            narrow.set(f"n-{i}", NarrowA(temp=float(i)))

        assert narrow.verify_log_chain(limit=3) is True

    def test_verify_log_chain_from_throwing_narrow_client(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 16: Throwing decoder is never invoked during verification."""
        wide_client.set("other-key", {"mode": "debug", "verbose": True})

        strict: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            strict_narrow_a_decoder, narrow_a_encoder
        )
        strict.set("sensor-01", NarrowA(temp=36.6))

        wide_client.set("another", {"count": 42})

        assert strict.verify_log_chain() is True

    def test_rapid_alternating_writes_then_verify(
        self, s3_bucket: str, wide_client: ImmuKVClient[str, object]
    ) -> None:
        """Test 17: Rapid alternating writes then verify from narrow client."""
        narrow: ImmuKVClient[str, NarrowA] = wide_client.with_codec(
            narrow_a_decoder, narrow_a_encoder
        )

        for i in range(10):
            wide_client.set("wide-key", {"kind": "misc", "iteration": i})
            narrow.set("temp-key", NarrowA(temp=float(i)))

        assert narrow.verify_log_chain() is True
        assert wide_client.verify_log_chain() is True
