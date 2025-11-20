/**
 * Internal type definitions not exposed in public API.
 */

import * as crypto from 'crypto';
import type { JSONValue } from '../jsonHelpers';
import { stringifyCanonical } from './jsonHelpers';
// Re-export these from parent for internal use
import type { Entry, Hash, Sequence, TimestampMs } from '../types';

// Generic branding utilities

export function brand<T, B extends string>(value: T): T & { readonly __brand: B } {
  return value as T & { readonly __brand: B };
}

export function brandWithKey<T, B extends string, K extends string>(
  value: T
): T & { readonly __brand: B; readonly __key: K } {
  return value as T & { readonly __brand: B; readonly __key: K };
}

/**
 * Type definition for log entry data used in hash calculation.
 */
export interface LogEntryForHash<K extends string = string, V = any> {
  sequence: Sequence<K>;
  key: K;
  value: V;
  timestampMs: TimestampMs<K>;
  previousHash: Hash<K>;
}

/**
 * Orphan status tracking.
 */
export interface OrphanStatus<K extends string = string, V = any> {
  /** True if latest entry is orphaned */
  isOrphaned: boolean;
  /** Key name of the orphaned entry (if orphaned) */
  orphanKey?: K;
  /** Full entry data (if orphaned) */
  orphanEntry?: Entry<K, V>;
  /** Timestamp when this check was performed */
  checkedAt: number;
}

// Factory functions for branded types

/**
 * Compute SHA-256 hash from log entry data.
 *
 * @param data - Log entry data to hash (excludes version_id, log_version_id, hash)
 * @returns Hash in format 'sha256:<64 hex characters>'
 */
export function hashCompute<K extends string, V>(data: LogEntryForHash<K, V>): Hash<K> {
  const canonical = stringifyCanonical(data as unknown as JSONValue);
  const hashBytes = crypto.createHash('sha256').update(canonical, 'utf-8').digest('hex');
  return brandWithKey<string, 'Hash', K>(`sha256:${hashBytes}`);
}

/**
 * Return genesis hash for the first entry in a chain.
 *
 * @returns Genesis hash 'sha256:genesis'
 */
export function hashGenesis<K extends string>(): Hash<K> {
  return brandWithKey<string, 'Hash', K>('sha256:genesis');
}

/**
 * Parse hash from JSON string with validation.
 *
 * @param s - Hash string from JSON
 * @returns Validated Hash type
 * @throws Error if hash format is invalid
 */
export function hashFromJson<K extends string>(s: string): Hash<K> {
  if (!s.startsWith('sha256:')) {
    throw new Error(`Invalid hash format (must start with 'sha256:'): ${s}`);
  }
  return brandWithKey<string, 'Hash', K>(s);
}

/**
 * Return initial sequence number before first entry.
 *
 * @returns Sequence number -1 (will become 0 on first write)
 */
export function sequenceInitial<K extends string>(): Sequence<K> {
  return brandWithKey<number, 'Sequence', K>(-1);
}

/**
 * Increment sequence number.
 *
 * @param seq - Current sequence number
 * @returns Next sequence number (seq + 1)
 */
export function sequenceNext<K extends string>(seq: Sequence<K>): Sequence<K> {
  return brandWithKey<number, 'Sequence', K>(seq + 1);
}

/**
 * Parse sequence from JSON with validation.
 *
 * @param n - Sequence number from JSON
 * @returns Validated Sequence type
 * @throws Error if sequence is invalid (< -1)
 */
export function sequenceFromJson<K extends string>(n: number): Sequence<K> {
  if (n < -1) {
    throw new Error(`Invalid sequence (must be >= -1): ${n}`);
  }
  return brandWithKey<number, 'Sequence', K>(n);
}

/**
 * Return current timestamp in milliseconds.
 *
 * @returns Current Unix epoch time in milliseconds
 */
export function timestampNow<K extends string>(): TimestampMs<K> {
  return brandWithKey<number, 'TimestampMs', K>(Date.now());
}

/**
 * Parse timestamp from JSON with validation.
 *
 * @param n - Timestamp in milliseconds from JSON
 * @returns Validated TimestampMs type
 * @throws Error if timestamp is invalid (<= 0)
 */
export function timestampFromJson<K extends string>(n: number): TimestampMs<K> {
  if (n <= 0) {
    throw new Error(`Invalid timestamp (must be > 0): ${n}`);
  }
  return brandWithKey<number, 'TimestampMs', K>(n);
}
