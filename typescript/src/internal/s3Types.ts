/**
 * S3-specific type definitions for internal use.
 *
 * These types are not part of the public API and should only be used internally.
 */

import * as s3 from '@aws-sdk/client-s3';
import type { StreamingBlobPayloadOutputTypes } from '@smithy/types';
import type { KeyObjectETag, KeyVersionId, LogVersionId } from '../types';
import { assertAwsFieldPresent } from './s3Helpers';
import { brandWithKey } from './types';

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
    return brandWithKey<string, 'S3KeyPath', LogKey>(`${prefix}_log.json`);
  },
} as const;

/**
 * GetObjectCommandOutput with corrected field optionality.
 *
 * AWS SDK types mark fields as optional due to Smithy bugs (GitHub issue #5992).
 * This type reflects actual AWS API behavior per documentation.
 */
export type GetObjectCommandOutput<K extends string> = {
  readonly __brand: 'GetObjectCommandOutput';
  readonly __key: K;
  Body: StreamingBlobPayloadOutputTypes; // Always returned per AWS docs
  ETag: string; // Always returned per AWS docs
  VersionId: string | undefined; // Optional (absent when versioning disabled)
};

/**
 * Helper functions for GetObjectCommandOutput.
 */
export const GetObjectCommandOutputs = {
  /**
   * Convert AWS SDK GetObjectCommandOutput to our GetObjectCommandOutput type.
   *
   * Reconstructs the response with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: <K extends string>(
    response: s3.GetObjectCommandOutput
  ): GetObjectCommandOutput<K> => {
    const body = assertAwsFieldPresent(response.Body, 'GetObjectCommandOutput.Body');
    const etag = assertAwsFieldPresent(response.ETag, 'GetObjectCommandOutput.ETag');
    return brandWithKey<
      {
        Body: typeof body;
        ETag: string;
        VersionId: string | undefined;
      },
      'GetObjectCommandOutput',
      K
    >({
      Body: body,
      ETag: etag,
      VersionId: response.VersionId,
    });
  },

  /**
   * Extract LogVersionId from GetObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: GetObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from GetObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(response: GetObjectCommandOutput<K>): KeyObjectETag<K> => {
    return brandWithKey<string, 'KeyObjectETag', K>(response.ETag);
  },
} as const;

/**
 * PutObjectCommandOutput with corrected field optionality.
 *
 * AWS SDK types mark fields as optional due to Smithy bugs (GitHub issue #5992).
 * This type reflects actual AWS API behavior per documentation.
 */
export type PutObjectCommandOutput<K extends string> = {
  readonly __brand: 'PutObjectCommandOutput';
  readonly __key: K;
  ETag: string; // Always returned per AWS docs
  VersionId: string | undefined; // Optional (absent when versioning disabled)
};

/**
 * Helper functions for PutObjectCommandOutput.
 */
export const PutObjectCommandOutputs = {
  /**
   * Convert AWS SDK PutObjectCommandOutput to our PutObjectCommandOutput type.
   *
   * Reconstructs the response with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: <K extends string>(
    response: s3.PutObjectCommandOutput
  ): PutObjectCommandOutput<K> => {
    return brandWithKey<
      {
        ETag: string;
        VersionId: string | undefined;
      },
      'PutObjectCommandOutput',
      K
    >({
      ETag: assertAwsFieldPresent(response.ETag, 'PutObjectCommandOutput.ETag'),
      VersionId: response.VersionId,
    });
  },

  /**
   * Extract LogVersionId from PutObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: PutObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from PutObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(response: PutObjectCommandOutput<K>): KeyObjectETag<K> => {
    return brandWithKey<string, 'KeyObjectETag', K>(response.ETag);
  },
} as const;

/**
 * HeadObjectCommandOutput with corrected field optionality.
 *
 * AWS SDK types mark fields as optional due to Smithy bugs (GitHub issue #5992).
 * This type reflects actual AWS API behavior per documentation.
 */
export type HeadObjectCommandOutput<K extends string> = {
  readonly __brand: 'HeadObjectCommandOutput';
  readonly __key: K;
  ETag: string; // Always returned per AWS docs
  VersionId: string | undefined; // Optional (absent when versioning disabled)
};

/**
 * Helper functions for HeadObjectCommandOutput.
 */
export const HeadObjectCommandOutputs = {
  /**
   * Convert AWS SDK HeadObjectCommandOutput to our HeadObjectCommandOutput type.
   *
   * Reconstructs the response with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: <K extends string>(
    response: s3.HeadObjectCommandOutput
  ): HeadObjectCommandOutput<K> => {
    return brandWithKey<
      {
        ETag: string;
        VersionId: string | undefined;
      },
      'HeadObjectCommandOutput',
      K
    >({
      ETag: assertAwsFieldPresent(response.ETag, 'HeadObjectCommandOutput.ETag'),
      VersionId: response.VersionId,
    });
  },

  /**
   * Extract LogVersionId from HeadObjectCommandOutput (for log operations).
   */
  logVersionId: <K extends string>(
    response: HeadObjectCommandOutput<LogKey>
  ): LogVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, 'LogVersionId', K>(response.VersionId)
      : undefined;
  },

  /**
   * Extract KeyObjectETag from HeadObjectCommandOutput (for key operations).
   */
  keyObjectEtag: <K extends string>(response: HeadObjectCommandOutput<K>): KeyObjectETag<K> => {
    return brandWithKey<string, 'KeyObjectETag', K>(response.ETag);
  },
} as const;

/**
 * ObjectVersion with corrected field optionality.
 *
 * AWS SDK types mark fields as optional due to Smithy bugs (GitHub issue #5992).
 * This type reflects actual AWS API behavior per documentation.
 */
export type ObjectVersion<K extends string> = {
  readonly __brand: 'ObjectVersion';
  readonly __key: K;
  Key: string; // Always returned per AWS docs
  VersionId: string; // Always returned per AWS docs
  IsLatest: boolean; // Always returned per AWS docs
  ETag: string; // Always returned per AWS docs
};

/**
 * Helper functions for ObjectVersion.
 */
export const ObjectVersions = {
  /**
   * Convert AWS SDK ObjectVersion to our ObjectVersion type.
   *
   * Reconstructs the object with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: <K extends string>(version: s3.ObjectVersion): ObjectVersion<K> => {
    return brandWithKey<
      {
        Key: string;
        VersionId: string;
        IsLatest: boolean;
        ETag: string;
      },
      'ObjectVersion',
      K
    >({
      Key: assertAwsFieldPresent(version.Key, 'ObjectVersion.Key'),
      VersionId: assertAwsFieldPresent(version.VersionId, 'ObjectVersion.VersionId'),
      IsLatest: assertAwsFieldPresent(version.IsLatest, 'ObjectVersion.IsLatest'),
      ETag: assertAwsFieldPresent(version.ETag, 'ObjectVersion.ETag'),
    });
  },

  /**
   * Extract LogVersionId from ObjectVersion (for log operations).
   */
  logVersionId: <K extends string>(version: ObjectVersion<LogKey>): LogVersionId<K> => {
    return brandWithKey<string, 'LogVersionId', K>(version.VersionId);
  },

  /**
   * Extract KeyVersionId from ObjectVersion (for key operations).
   */
  keyVersionId: <K extends string>(version: ObjectVersion<K>): KeyVersionId<K> => {
    return brandWithKey<string, 'KeyVersionId', K>(version.VersionId);
  },
} as const;

/**
 * ListObjectVersionsCommandOutput with corrected field optionality (helper type for return).
 */
export type ListObjectVersionsCommandOutput<K extends string> = {
  Versions: ObjectVersion<K>[] | undefined;
  IsTruncated: boolean;
  NextKeyMarker: string | undefined;
  NextVersionIdMarker: string | undefined;
};

/**
 * Helper functions for ListObjectVersionsCommandOutput.
 */
export const ListObjectVersionsCommandOutputs = {
  /**
   * Convert AWS SDK ListObjectVersionsCommandOutput to our type.
   *
   * Reconstructs the response with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: <K extends string>(
    response: s3.ListObjectVersionsCommandOutput
  ): ListObjectVersionsCommandOutput<K> => {
    return {
      IsTruncated: assertAwsFieldPresent(
        response.IsTruncated,
        'ListObjectVersionsCommandOutput.IsTruncated'
      ),
      NextKeyMarker: response.NextKeyMarker,
      NextVersionIdMarker: response.NextVersionIdMarker,
      Versions:
        response.Versions !== undefined
          ? response.Versions.map(v => ObjectVersions.fromAwsSdk(v))
          : undefined,
    };
  },
} as const;

/**
 * Object summary in ListObjectsV2 response with corrected field optionality.
 *
 * Matches AWS SDK _Object type name but with corrected optionality.
 * This type reflects actual AWS API behavior per documentation.
 */
export type _Object = {
  Key: string; // Always returned per AWS docs
};

/**
 * ListObjectsV2CommandOutput with corrected field optionality.
 *
 * AWS SDK types mark fields as optional due to Smithy bugs (GitHub issue #5992).
 * This type reflects actual AWS API behavior per documentation.
 */
export type ListObjectsV2CommandOutput = {
  Contents: _Object[] | undefined; // Optional (can be empty)
  IsTruncated: boolean; // Always returned per AWS docs
  NextContinuationToken: string | undefined; // Optional (only when IsTruncated=true)
};

/**
 * Helper functions for ListObjectsV2CommandOutput.
 */
export const ListObjectsV2CommandOutputs = {
  /**
   * Convert AWS SDK ListObjectsV2CommandOutput to our ListObjectsV2CommandOutput type.
   *
   * Reconstructs the response with correct field optionality, asserting
   * that fields which should always be present are actually present.
   */
  fromAwsSdk: (response: s3.ListObjectsV2CommandOutput): ListObjectsV2CommandOutput => {
    return {
      IsTruncated: assertAwsFieldPresent(
        response.IsTruncated,
        'ListObjectsV2CommandOutput.IsTruncated'
      ),
      NextContinuationToken: response.NextContinuationToken,
      Contents:
        response.Contents !== undefined
          ? response.Contents.map(obj => ({
              Key: assertAwsFieldPresent(obj.Key, '_Object.Key'),
            }))
          : undefined,
    };
  },
} as const;
