/**
 * Internal type definitions not exposed in public API.
 */

import type { ContentHash, FileS3Key, FileVersionId } from "../types";

// Generic branding utilities

export function brand<T, B extends string>(
  value: T,
): T & { readonly __brand: B } {
  return value as T & { readonly __brand: B };
}

export function brandWithKey<T, B extends string, K extends string>(
  value: T,
): T & { readonly __brand: B; readonly __key: K } {
  return value as T & { readonly __brand: B; readonly __key: K };
}

// Factory functions for branded types

/**
 * Parse content hash from JSON string with validation.
 *
 * @param s - Hash string from JSON
 * @returns Validated ContentHash type
 * @throws Error if hash format is invalid
 */
export function contentHashFromJson<K extends string>(
  s: string,
): ContentHash<K> {
  if (!s.startsWith("sha256:") || s.length !== 71) {
    throw new Error(
      `Invalid content hash format (expected 'sha256:' + 64 hex chars): ${s}`,
    );
  }
  return brandWithKey<string, "ContentHash", K>(s);
}

/**
 * Create content hash from computed hex digest.
 *
 * @param hexDigest - 64-character hex SHA-256 digest
 * @returns ContentHash with 'sha256:' prefix
 */
export function contentHashFromDigest<K extends string>(
  hexDigest: string,
): ContentHash<K> {
  if (hexDigest.length !== 64 || !/^[0-9a-f]+$/.test(hexDigest)) {
    throw new Error(`Invalid hex digest (expected 64 hex chars): ${hexDigest}`);
  }
  return brandWithKey<string, "ContentHash", K>(`sha256:${hexDigest}`);
}

/**
 * Check if a string is a valid content hash format.
 *
 * @param s - String to validate
 * @returns true if valid content hash format
 */
export function isValidContentHash(s: string): boolean {
  return /^sha256:[0-9a-f]{64}$/.test(s);
}

/**
 * Create FileVersionId from AWS SDK version ID.
 *
 * @param versionId - S3 version ID from AWS SDK
 * @returns Branded FileVersionId
 */
export function fileVersionIdFromAwsSdk<K extends string>(
  versionId: string,
): FileVersionId<K> {
  return brandWithKey<string, "FileVersionId", K>(versionId);
}

/**
 * Parse FileVersionId from JSON string.
 *
 * @param s - Version ID string from JSON
 * @returns Validated FileVersionId type
 */
export function fileVersionIdFromJson<K extends string>(
  s: string,
): FileVersionId<K> {
  return brandWithKey<string, "FileVersionId", K>(s);
}

/**
 * Factory methods for creating file S3 key paths.
 */
export const FileS3Keys = {
  /**
   * Create S3 path for a file object.
   *
   * @param prefix - S3 key prefix (e.g., "files/")
   * @param key - User-supplied file key
   * @returns Branded FileS3Key
   */
  forFile: <K extends string>(prefix: string, key: K): FileS3Key<K> => {
    return brandWithKey<string, "FileS3Key", K>(`${prefix}${key}`);
  },
} as const;

/**
 * Factory methods for ContentHash operations.
 */
export const ContentHashes = {
  /**
   * Create ContentHash from hex digest.
   */
  fromDigest: <K extends string>(hexDigest: string): ContentHash<K> => {
    return contentHashFromDigest(hexDigest);
  },

  /**
   * Parse ContentHash from JSON.
   */
  fromJson: <K extends string>(s: string): ContentHash<K> => {
    return contentHashFromJson(s);
  },

  /**
   * Check if string is valid content hash format.
   */
  isValid: (s: string): boolean => {
    return isValidContentHash(s);
  },
} as const;

/**
 * Result of file upload operation.
 * Cached to prevent orphan multiplication on retry.
 */
export interface UploadResult<K extends string> {
  /** S3 key where file was uploaded */
  s3Key: FileS3Key<K>;

  /** S3 version ID assigned by S3 */
  s3VersionId: FileVersionId<K>;

  /** SHA-256 hash of uploaded content */
  contentHash: ContentHash<K>;

  /** File size in bytes */
  contentLength: number;

  /** Content type */
  contentType: string;

  /** User metadata if provided */
  userMetadata?: Record<string, string>;
}
