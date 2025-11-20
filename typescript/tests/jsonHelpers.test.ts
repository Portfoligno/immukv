/**
 * Tests for JSON helper functions.
 */

import type { JSONValue } from '../src/jsonHelpers';
import { stringifyCanonical } from '../src/internal/jsonHelpers';

describe('stringifyCanonical', () => {
  test('basic serialization with sorted keys', () => {
    const data = { key: 'value', sequence: 42, timestamp_ms: 1234567890 };
    const result = stringifyCanonical(data);

    // Should be a string
    expect(typeof result).toBe('string');

    // Should be valid JSON with sorted keys
    expect(result).toBe('{"key":"value","sequence":42,"timestamp_ms":1234567890}');
  });

  test('different key orders produce identical output', () => {
    // Same data, different key order
    const data1 = { a: 1, b: 2, c: 3 };
    const data2 = { c: 3, a: 1, b: 2 };
    const data3 = { b: 2, c: 3, a: 1 };

    const result1 = stringifyCanonical(data1);
    const result2 = stringifyCanonical(data2);
    const result3 = stringifyCanonical(data3);

    // All should produce identical output
    expect(result1).toBe(result2);
    expect(result2).toBe(result3);

    // Should be sorted alphabetically
    expect(result1).toBe('{"a":1,"b":2,"c":3}');
  });

  test('nested objects have sorted keys', () => {
    const data = {
      outer_z: { nested_z: 3, nested_a: 1 },
      outer_a: { nested_y: 2, nested_x: 1 },
    };

    const result = stringifyCanonical(data);

    // Outer keys should be sorted
    expect(result.startsWith('{"outer_a":')).toBe(true);

    // Inner keys should also be sorted
    expect(result).toContain('"nested_x":1,"nested_y":2');
    expect(result).toContain('"nested_a":1,"nested_z":3');
  });

  test('arrays preserve order (not sorted)', () => {
    const data = { items: [3, 1, 2], key: 'value' };

    const result = stringifyCanonical(data);

    // Array order should be preserved
    expect(result).toContain('"items":[3,1,2]');

    // But keys should still be sorted
    expect(result).toBe('{"items":[3,1,2],"key":"value"}');
  });

  test('output has no extra whitespace', () => {
    const data = {
      key1: 'value1',
      key2: { nested: 'value2' },
      key3: [1, 2, 3],
    };

    const result = stringifyCanonical(data);

    // Should have no spaces, newlines, or tabs
    expect(result).not.toContain(' ');
    expect(result).not.toContain('\n');
    expect(result).not.toContain('\t');
  });

  test('special JSON values serialize correctly', () => {
    const data = {
      null_value: null,
      bool_true: true,
      bool_false: false,
      zero: 0,
      empty_string: '',
      empty_array: [],
      empty_object: {},
    };

    const result = stringifyCanonical(data);

    // Verify special values are correctly serialized
    expect(result).toContain('"null_value":null');
    expect(result).toContain('"bool_true":true');
    expect(result).toContain('"bool_false":false');
    expect(result).toContain('"zero":0');
    expect(result).toContain('"empty_string":""');
    expect(result).toContain('"empty_array":[]');
    expect(result).toContain('"empty_object":{}');
  });

  test('unicode characters are escaped as ASCII', () => {
    const data = { message: 'Hello 世界', emoji: '🤖' };

    const result = stringifyCanonical(data);

    // Should be a valid string
    expect(typeof result).toBe('string');

    // Unicode characters should be escaped as \uXXXX sequences
    expect(result).toContain('\\u4e16\\u754c'); // 世界
    expect(result).toContain('\\ud83e\\udd16'); // 🤖 (surrogate pair)

    // Should not contain literal non-ASCII characters
    expect(result).not.toContain('世');
    expect(result).not.toContain('🤖');
  });

  test('multiple calls with same data produce identical output', () => {
    const data = {
      key: 'value',
      nested: { z: 3, a: 1, m: 2 },
      array: [1, 2, 3],
      number: 42,
      bool: true,
      null: null,
    };

    // Call multiple times
    const result1 = stringifyCanonical(data);
    const result2 = stringifyCanonical(data);
    const result3 = stringifyCanonical(data);

    // All should be identical
    expect(result1).toBe(result2);
    expect(result2).toBe(result3);
  });

  test('real-world entry data structure', () => {
    const entryData = {
      sequence: 42,
      key: 'sensor-012352',
      value: { alpha: 0.15, beta: 2.8 },
      timestamp_ms: 1729765800000,
      previous_version_id: 'wGbM3BFnS1P.8ldAZKnkKj6B6FD6vrA',
      previous_hash: 'sha256:a1b2c3d4e5f6789',
      hash: 'sha256:d4e5f6a7b8c9def',
      previous_key_object_etag: '"abc123"',
    };

    const result = stringifyCanonical(entryData);

    // Should have all keys sorted alphabetically
    const keys = [
      'hash',
      'key',
      'previous_hash',
      'previous_key_object_etag',
      'previous_version_id',
      'sequence',
      'timestamp_ms',
      'value',
    ];

    // Check keys appear in sorted order by checking their positions
    const positions = keys.map(k => result.indexOf(`"${k}":`));
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
  });

  test('deeply nested objects maintain sort order at all levels', () => {
    const data = {
      z: {
        z: {
          z: 3,
          a: 1,
        },
        a: 2,
      },
      a: 1,
    };

    const result = stringifyCanonical(data);

    // All levels should be sorted
    expect(result).toBe('{"a":1,"z":{"a":2,"z":{"a":1,"z":3}}}');
  });

  test('arrays of objects have sorted keys', () => {
    const data: JSONValue = {
      items: [
        { z: 3, a: 1 },
        { y: 2, x: 1 },
      ],
    };

    const result = stringifyCanonical(data);

    // Objects within arrays should also have sorted keys
    expect(result).toBe('{"items":[{"a":1,"z":3},{"x":1,"y":2}]}');
  });

  test('mixed array contents preserve order', () => {
    const data = {
      mixed: [null, true, 'string', 42, { z: 2, a: 1 }, [3, 2, 1]],
    };

    const result = stringifyCanonical(data);

    // Array order preserved, but object keys sorted
    expect(result).toBe('{"mixed":[null,true,"string",42,{"a":1,"z":2},[3,2,1]]}');
  });
});
