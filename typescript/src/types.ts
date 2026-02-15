/**
 * Type definitions for ImmuKV.
 */

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
 * Static credentials with access key, secret key, and optional session token.
 */
export interface StaticCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  /** STS session token for temporary credentials */
  sessionToken?: string;
}

/**
 * Async credential provider function.
 * Returns a promise resolving to static credentials.
 * The AWS SDK v3 S3Client accepts this natively as a credential provider.
 */
export type CredentialProvider = () => Promise<StaticCredentials>;

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
  /** Override default S3 client behavior (for MinIO in production, or testing with LocalStack/moto) */
  overrides?: {
    /** Custom S3 endpoint URL */
    endpointUrl?: string;
    /** Explicit credentials: static object or async provider function (not needed for AWS with IAM roles) */
    credentials?: StaticCredentials | CredentialProvider;
    /** Use path-style URLs instead of virtual-hosted style (required for MinIO) */
    forcePathStyle?: boolean;
  };
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
