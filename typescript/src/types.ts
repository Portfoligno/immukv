/**
 * Type definitions for ImmuKV.
 */

import * as crypto from 'crypto';
import { stringifyCanonical, JSONValue } from './jsonHelpers';

// Generic branding utilities

/**
 * Brand a value with a type marker (for types without key parameter).
 */
export function brand<T, B extends string>(value: T): T & { readonly __brand: B } {
  return value as T & { readonly __brand: B };
}

/**
 * Brand a value with type and key markers (for types with key parameter).
 */
export function brandWithKey<T, B extends string, K extends string>(
  value: T
): T & { readonly __brand: B; readonly __key: K } {
  return value as T & { readonly __brand: B; readonly __key: K };
}

// Branded types parameterized by key type
// These are branded types to prevent mixing version IDs from different contexts
export type LogVersionId<K extends string> = string & {
  readonly __brand: 'LogVersionId';
  readonly __key: K;
};
export type KeyVersionId<K extends string> = string & {
  readonly __brand: 'KeyVersionId';
  readonly __key: K;
};
export type KeyObjectETag<K extends string> = string & {
  readonly __brand: 'KeyObjectETag';
  readonly __key: K;
};
export type Hash<K extends string> = string & {
  readonly __brand: 'Hash';
  readonly __key: K;
};
export type Sequence<K extends string> = number & {
  readonly __brand: 'Sequence';
  readonly __key: K;
};
export type TimestampMs<K extends string> = number & {
  readonly __brand: 'TimestampMs';
  readonly __key: K;
};

/**
 * Client configuration.
 */
export interface Config {
  /** S3 bucket name (required) */
  s3Bucket: string;
  /** S3 region (required) */
  s3Region: string;
  /** S3 key prefix (required, use empty string for no prefix) */
  s3Prefix: string;
  /** Optional KMS key ID for encryption */
  kmsKeyId?: string;
  /** Orphan repair check interval in milliseconds (default: 300000 = 5 minutes) */
  repairCheckIntervalMs?: number;
  /** Read-only mode - disables all repair attempts (default: false) */
  readOnly?: boolean;
}

/**
 * Log entry representation.
 */
export interface Entry<K extends string = string, V = any> {
  /** The key */
  key: K;
  /** The value */
  value: V;
  /** Timestamp in epoch milliseconds */
  timestampMs: TimestampMs<K>;
  /** Log version ID for this entry */
  versionId: LogVersionId<K>;
  /** Client-maintained sequence counter */
  sequence: Sequence<K>;
  /** Previous log version ID */
  previousVersionId?: LogVersionId<K>;
  /** SHA-256 hash of this entry */
  hash: Hash<K>;
  /** Hash from previous entry */
  previousHash: Hash<K>;
  /** Previous key object ETag at log write time (for repair) */
  previousKeyObjectEtag?: KeyObjectETag<K>;
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

/**
 * Error thrown when a key is not found.
 */
export class KeyNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'KeyNotFoundError';
  }
}

/**
 * Error thrown when attempting to write in read-only mode.
 */
export class ReadOnlyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ReadOnlyError';
  }
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
