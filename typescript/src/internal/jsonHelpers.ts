/**
 * Internal JSON helper functions not exposed in public API.
 */

import type { JSONValue } from '../jsonHelpers';

/**
 * Escape non-ASCII characters in a string as \uXXXX sequences.
 *
 * This ensures deterministic serialization across all platforms and languages,
 * avoiding Unicode normalization issues.
 *
 * @param str - The string to escape
 * @returns String with non-ASCII characters escaped
 */
function escapeNonAscii(str: string): string {
  // eslint-disable-next-line no-control-regex
  return str.replace(/[^\x00-\x7F]/g, char => {
    const code = char.charCodeAt(0);
    return '\\u' + code.toString(16).padStart(4, '0');
  });
}

/**
 * Serialize data to canonical JSON format for S3 storage.
 *
 * Uses sorted keys and minimal separators for deterministic serialization.
 * This ensures consistent ETags for idempotent repair operations.
 *
 * Escapes all non-ASCII characters as \uXXXX to avoid Unicode normalization
 * issues and ensure deterministic output across all platforms.
 *
 * @param data - The data to serialize
 * @returns ASCII-only JSON string ready for S3 upload
 */
export function stringifyCanonical(data: JSONValue): string {
  const json = JSON.stringify(data, (key, value) => {
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      // Sort object keys for deterministic serialization
      return Object.keys(value)
        .sort()
        .reduce((sorted: { [key: string]: JSONValue }, k: string) => {
          sorted[k] = value[k];
          return sorted;
        }, {});
    }
    return value;
  });

  // Escape non-ASCII characters to match Python's ensure_ascii=True behavior
  return escapeNonAscii(json);
}
