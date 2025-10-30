/**
 * Type definitions for ImmuKV.
 */

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
  timestampMs: number;
  /** Log version ID for this entry */
  versionId: string;
  /** Client-maintained sequence counter */
  sequence: number;
  /** Previous log version ID */
  previousVersionId?: string;
  /** SHA-256 hash of this entry */
  hash: string;
  /** Hash from previous entry */
  previousHash: string;
  /** Previous key object ETag at log write time (for repair) */
  previousKeyObjectEtag?: string;
}

/**
 * Type definition for log entry data used in hash calculation.
 */
export interface LogEntryForHash<K extends string = string, V = any> {
  sequence: number;
  key: K;
  value: V;
  timestampMs: number;
  previousHash: string;
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
