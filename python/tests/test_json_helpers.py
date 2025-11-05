"""Tests for JSON helper functions."""

import pytest
from immukv.json_helpers import JSONValue, dumps_canonical


def test_dumps_canonical_basic() -> None:
    """Test basic serialization with sorted keys."""
    data: JSONValue = {"key": "value", "sequence": 42, "timestamp_ms": 1234567890}
    result = dumps_canonical(data)

    # Should be bytes
    assert isinstance(result, bytes)

    # Should be UTF-8 encoded JSON
    decoded = result.decode("utf-8")
    assert decoded == '{"key":"value","sequence":42,"timestamp_ms":1234567890}'


def test_dumps_canonical_key_order_independence() -> None:
    """Test that different key orders produce identical output."""
    # Same data, different key order
    data1: JSONValue = {"a": 1, "b": 2, "c": 3}
    data2: JSONValue = {"c": 3, "a": 1, "b": 2}
    data3: JSONValue = {"b": 2, "c": 3, "a": 1}

    result1 = dumps_canonical(data1)
    result2 = dumps_canonical(data2)
    result3 = dumps_canonical(data3)

    # All should produce identical output
    assert result1 == result2 == result3

    # Should be sorted alphabetically
    decoded = result1.decode("utf-8")
    assert decoded == '{"a":1,"b":2,"c":3}'


def test_dumps_canonical_nested_objects() -> None:
    """Test serialization of nested objects with key sorting."""
    data: JSONValue = {
        "outer_z": {"nested_z": 3, "nested_a": 1},
        "outer_a": {"nested_y": 2, "nested_x": 1},
    }

    result = dumps_canonical(data)
    decoded = result.decode("utf-8")

    # Outer keys should be sorted
    assert decoded.startswith('{"outer_a":')

    # Inner keys should also be sorted
    assert '"nested_x":1,"nested_y":2' in decoded
    assert '"nested_a":1,"nested_z":3' in decoded


def test_dumps_canonical_arrays() -> None:
    """Test that arrays preserve order (not sorted)."""
    data: JSONValue = {"items": [3, 1, 2], "key": "value"}

    result = dumps_canonical(data)
    decoded = result.decode("utf-8")

    # Array order should be preserved
    assert '"items":[3,1,2]' in decoded

    # But keys should still be sorted
    assert decoded == '{"items":[3,1,2],"key":"value"}'


def test_dumps_canonical_no_whitespace() -> None:
    """Test that output has no extra whitespace."""
    data: JSONValue = {
        "key1": "value1",
        "key2": {"nested": "value2"},
        "key3": [1, 2, 3],
    }

    result = dumps_canonical(data)
    decoded = result.decode("utf-8")

    # Should have no spaces after colons or commas
    assert " " not in decoded
    assert "\n" not in decoded
    assert "\t" not in decoded


def test_dumps_canonical_special_values() -> None:
    """Test serialization of special JSON values."""
    data: JSONValue = {
        "null_value": None,
        "bool_true": True,
        "bool_false": False,
        "zero": 0,
        "empty_string": "",
        "empty_array": [],
        "empty_object": {},
    }

    result = dumps_canonical(data)
    decoded = result.decode("utf-8")

    # Verify special values are correctly serialized
    assert '"null_value":null' in decoded
    assert '"bool_true":true' in decoded
    assert '"bool_false":false' in decoded
    assert '"zero":0' in decoded
    assert '"empty_string":""' in decoded
    assert '"empty_array":[]' in decoded
    assert '"empty_object":{}' in decoded


def test_dumps_canonical_unicode() -> None:
    """Test that Unicode characters are escaped as ASCII."""
    data: JSONValue = {"message": "Hello 世界", "emoji": "🤖"}

    result = dumps_canonical(data)

    # Should be valid UTF-8 bytes
    assert isinstance(result, bytes)

    # Should decode to ASCII-only JSON with escaped sequences
    decoded = result.decode("utf-8")

    # Unicode characters should be escaped as \uXXXX sequences
    assert "\\u4e16\\u754c" in decoded  # 世界
    assert "\\ud83e\\udd16" in decoded  # 🤖 (surrogate pair)

    # Should not contain literal non-ASCII characters
    assert "世" not in decoded
    assert "🤖" not in decoded


def test_dumps_canonical_deterministic() -> None:
    """Test that multiple calls with same data produce identical output."""
    data: JSONValue = {
        "key": "value",
        "nested": {"z": 3, "a": 1, "m": 2},
        "array": [1, 2, 3],
        "number": 42,
        "bool": True,
        "null": None,
    }

    # Call multiple times
    result1 = dumps_canonical(data)
    result2 = dumps_canonical(data)
    result3 = dumps_canonical(data)

    # All should be identical
    assert result1 == result2 == result3


def test_dumps_canonical_real_world_entry() -> None:
    """Test with real-world entry data structure."""
    entry_data: JSONValue = {
        "sequence": 42,
        "key": "sensor-012352",
        "value": {"alpha": 0.15, "beta": 2.8},
        "timestamp_ms": 1729765800000,
        "previous_version_id": "wGbM3BFnS1P.8ldAZKnkKj6B6FD6vrA",
        "previous_hash": "sha256:a1b2c3d4e5f6789",
        "hash": "sha256:d4e5f6a7b8c9def",
        "previous_key_object_etag": '"abc123"',
    }

    result = dumps_canonical(entry_data)
    decoded = result.decode("utf-8")

    # Should have all keys sorted alphabetically
    keys = [
        "hash",
        "key",
        "previous_hash",
        "previous_key_object_etag",
        "previous_version_id",
        "sequence",
        "timestamp_ms",
        "value",
    ]

    # Check keys appear in sorted order by checking their positions
    positions = [decoded.index(f'"{k}":') for k in keys]
    assert positions == sorted(positions), "Keys should appear in alphabetical order"
