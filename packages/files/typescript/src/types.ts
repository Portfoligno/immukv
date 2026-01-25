/**
 * Type definitions for ImmuKV file storage.
 */

import type { Entry, KeyVersionId } from "immukv";

// Branded types parameterized by key type
// These are branded types to prevent mixing values from different contexts

/**
 * SHA-256 content hash, prefixed with "sha256:".
 * Key-parameterized to prevent cross-key hash confusion.
 */
export type ContentHash<K extends string> = string & {
  readonly __brand: "ContentHash";
  readonly __key: K;
};

/**
 * S3 version ID for file objects.
 * Distinct from LogVersionId and KeyVersionId to prevent accidental mixing.
 */
export type FileVersionId<K extends string> = string & {
  readonly __brand: "FileVersionId";
  readonly __key: K;
};

/**
 * S3 key path for file objects.
 * Type-safe path construction prevents path injection.
 */
export type FileS3Key<K extends string> = string & {
  readonly __brand: "FileS3Key";
  readonly __key: K;
};

/**
 * Configuration for file storage destination.
 * If omitted entirely, files stored in same bucket as ImmuKV log.
 */
export interface FileStorageConfig {
  /** S3 bucket for files. If omitted, uses same bucket as ImmuKV log. */
  bucket?: string;

  /** S3 region for files. If omitted, uses same region as log. */
  region?: string;

  /** S3 key prefix for files. Default: "files/" for same bucket, "" for different bucket. */
  prefix?: string;

  /** KMS key for file encryption. */
  kmsKeyId?: string;

  /** S3 client overrides (endpoint, credentials, pathStyle). */
  overrides?: {
    endpointUrl?: string;
    credentials?: { accessKeyId: string; secretAccessKey: string };
    forcePathStyle?: boolean;
  };

  /** Validate bucket access at construction. Default: true */
  validateAccess?: boolean;

  /** Validate bucket versioning is enabled. Default: true */
  validateVersioning?: boolean;
}

/**
 * Metadata for an active file.
 * Bucket is determined by FileClient configuration, not stored per-entry.
 */
export interface FileMetadata<K extends string> {
  /** Discriminant: explicitly absent for active files */
  readonly deleted?: never;

  /** S3 key within the configured bucket */
  s3Key: FileS3Key<K>;

  /** S3 version ID for immutable reference */
  s3VersionId: FileVersionId<K>;

  /** SHA-256 hash of file content (audit-grade integrity) */
  contentHash: ContentHash<K>;

  /** File size in bytes */
  contentLength: number;

  /** MIME type */
  contentType: string;

  /** Optional user-defined metadata */
  userMetadata?: Record<string, string>;
}

/**
 * Metadata for a deleted file (tombstone).
 * Use history() to see what content was deleted.
 */
export interface DeletedFileMetadata<K extends string> {
  /** Discriminant: true for tombstones */
  readonly deleted: true;

  /** S3 key within the configured bucket */
  s3Key: FileS3Key<K>;

  /**
   * S3 delete marker's version ID.
   *
   * When a file is deleted from a versioned S3 bucket, S3 creates a "delete marker"
   * instead of permanently removing the object. This field stores the delete marker's
   * version ID, not the original file's version ID. The original file content remains
   * accessible via S3 versioning for audit purposes.
   *
   * Use history() to find the original file's s3VersionId and contentHash.
   */
  deletedVersionId: FileVersionId<K>;
}

/**
 * Union type for file value (active or deleted).
 */
export type FileValue<K extends string> =
  | FileMetadata<K>
  | DeletedFileMetadata<K>;

/**
 * Entry type for file operations.
 */
export type FileEntry<K extends string> = Entry<K, FileValue<K>>;

/**
 * Options for setFile() operation.
 */
export interface SetFileOptions {
  /** MIME type. If omitted, defaults to 'application/octet-stream'. */
  contentType?: string;

  /** User-defined metadata to store with the file. */
  userMetadata?: Record<string, string>;
}

/**
 * Options for getFile() operation.
 */
export interface GetFileOptions<K extends string> {
  /** Version ID for historical access. If omitted, returns latest active version. */
  versionId?: KeyVersionId<K>;
}

/**
 * Return type for getFile().
 */
export interface FileDownload<K extends string> {
  /** The file entry metadata from the log. */
  entry: FileEntry<K>;

  /** Readable stream of file content. */
  stream: NodeJS.ReadableStream;
}

/**
 * Type guard to check if a file value represents a deleted file.
 */
export function isDeletedFile<K extends string>(
  value: FileValue<K>,
): value is DeletedFileMetadata<K> {
  return value.deleted === true;
}

/**
 * Type guard to check if a file value represents an active file.
 */
export function isActiveFile<K extends string>(
  value: FileValue<K>,
): value is FileMetadata<K> {
  return value.deleted !== true;
}

/**
 * Error thrown when a file is not found.
 */
export class FileNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FileNotFoundError";
  }
}

/**
 * Error thrown when accessing a deleted file.
 */
export class FileDeletedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FileDeletedError";
  }
}

/**
 * Error thrown when file content hash does not match.
 */
export class IntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IntegrityError";
  }
}

/**
 * Error thrown when file exists in log but S3 version is missing.
 */
export class FileOrphanedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FileOrphanedError";
  }
}

/**
 * Error thrown for invalid configuration.
 */
export class ConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConfigurationError";
  }
}

/**
 * Error thrown when setFile() retry limit is reached.
 */
export class MaxRetriesExceededError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MaxRetriesExceededError";
  }
}
