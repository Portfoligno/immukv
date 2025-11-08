/**
 * S3-specific type definitions for internal use.
 *
 * These types are not part of the public API and should only be used internally.
 */

import * as s3 from '@aws-sdk/client-s3';
import { LogVersionId, KeyVersionId, KeyObjectETag, brandWithKey } from '../types';

/**
 * Branded type for log keys (constant key type for log file paths).
 */
export type LogKey = string & {
  readonly __brand: 'LogKey';
};

/**
 * Branded S3 path string carrying the key type K.
 */
export type S3KeyPath<K extends string> = string & {
  readonly __brand: 'S3KeyPath';
  readonly __key: K;
};

/**
 * Factory methods for creating S3 key paths.
 */
export const S3KeyPaths = {
  /**
   * Create S3 path for a key object.
   */
  forKey: <K extends string>(prefix: string, key: K): S3KeyPath<K> => {
    return brandWithKey<string, 'S3KeyPath', K>(`${prefix}keys/${key}.json`);
  },

  /**
   * Create S3 path for the log file.
   */
  forLog: (prefix: string): S3KeyPath<LogKey> => {
    return `${prefix}_log.json` as S3KeyPath<LogKey>;
  },
} as const;

/**
 * Branded GetObjectCommandOutput for nominal typing.
 */
export type GetObjectCommandOutput<K extends string> = s3.GetObjectCommandOutput & {
  readonly __brand: 'GetObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for GetObjectCommandOutput.
 */
export const GetObjectCommandOutputs = {
  /**
   * Extract LogVersionId from GetObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: GetObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from GetObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(
    response: GetObjectCommandOutput<K>
  ): KeyObjectETag<K> | undefined => {
    return response.ETag ? brandWithKey<string, 'KeyObjectETag', K>(response.ETag) : undefined;
  },
} as const;

/**
 * Branded PutObjectCommandOutput for nominal typing.
 */
export type PutObjectCommandOutput<K extends string> = s3.PutObjectCommandOutput & {
  readonly __brand: 'PutObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for PutObjectCommandOutput.
 */
export const PutObjectCommandOutputs = {
  /**
   * Extract LogVersionId from PutObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: PutObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from PutObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(
    response: PutObjectCommandOutput<K>
  ): KeyObjectETag<K> | undefined => {
    return response.ETag ? brandWithKey<string, 'KeyObjectETag', K>(response.ETag) : undefined;
  },
} as const;

/**
 * Branded HeadObjectCommandOutput for nominal typing.
 */
export type HeadObjectCommandOutput<K extends string> = s3.HeadObjectCommandOutput & {
  readonly __brand: 'HeadObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for HeadObjectCommandOutput.
 */
export const HeadObjectCommandOutputs = {
  /**
   * Extract LogVersionId from HeadObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: HeadObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from HeadObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(
    response: HeadObjectCommandOutput<K>
  ): KeyObjectETag<K> | undefined => {
    return response.ETag ? brandWithKey<string, 'KeyObjectETag', K>(response.ETag) : undefined;
  },
} as const;

/**
 * Branded ObjectVersion for nominal typing.
 */
export type ObjectVersion<K extends string> = s3.ObjectVersion & {
  readonly __brand: 'ObjectVersion';
  readonly __key: K;
};

/**
 * Helper functions for ObjectVersion.
 */
export const ObjectVersions = {
  /**
   * Extract LogVersionId from ObjectVersion (for log operations).
   */
  logVersionId: <K extends string>(version: ObjectVersion<LogKey>): LogVersionId<K> | undefined => {
    return version.VersionId
      ? brandWithKey<string, 'LogVersionId', K>(version.VersionId)
      : undefined;
  },

  /**
   * Extract KeyVersionId from ObjectVersion (for key operations).
   */
  keyVersionId: <K extends string>(version: ObjectVersion<K>): KeyVersionId<K> | undefined => {
    return version.VersionId
      ? brandWithKey<string, 'KeyVersionId', K>(version.VersionId)
      : undefined;
  },
} as const;
