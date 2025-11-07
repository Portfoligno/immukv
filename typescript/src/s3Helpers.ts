/**
 * Helper functions for S3 operations.
 *
 * Centralizes type handling for AWS SDK responses to maintain type safety.
 */

import * as s3 from '@aws-sdk/client-s3';
import { JSONValue } from './jsonHelpers';
import { LogVersionId, KeyVersionId, KeyObjectETag, brandWithKey } from './types';

/**
 * Branded type for log keys (constant key type for log file paths).
 * @internal
 */
export type LogKey = string & {
  readonly __brand: 'LogKey';
};

/**
 * Branded S3 path string carrying the key type K.
 * @internal
 */
export type S3KeyPath<K extends string> = string & {
  readonly __brand: 'S3KeyPath';
  readonly __key: K;
};

/**
 * Factory methods for creating S3 key paths.
 * @internal
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
 * Read S3 Body stream and parse as JSON.
 *
 * Centralizes JSON parsing from S3 GetObject responses.
 *
 * @param body - S3 response body stream
 * @returns Parsed JSON object
 * @throws Error if body is undefined or JSON parsing fails
 * @internal
 */
export async function readBodyAsJson(
  body: s3.GetObjectCommandOutput['Body']
): Promise<{ [key: string]: JSONValue }> {
  if (!body) {
    throw new Error('S3 response body is undefined');
  }
  const bodyString = await body.transformToString();
  return JSON.parse(bodyString) as { [key: string]: JSONValue };
}

/**
 * Extract error code from AWS SDK error.
 *
 * Centralizes error code extraction to handle different error formats.
 *
 * @param error - AWS SDK error object
 * @returns Error code string
 * @internal
 */
export function getErrorCode(error: any): string {
  return error.name || error.$metadata?.httpStatusCode?.toString() || 'Unknown';
}

// Branded AWS SDK response types and their helpers

/**
 * Branded GetObjectCommandOutput for nominal typing.
 * @internal
 */
export type GetObjectCommandOutput<K extends string> = s3.GetObjectCommandOutput & {
  readonly __brand: 'GetObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for GetObjectCommandOutput.
 * @internal
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
 * @internal
 */
export type PutObjectCommandOutput<K extends string> = s3.PutObjectCommandOutput & {
  readonly __brand: 'PutObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for PutObjectCommandOutput.
 * @internal
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
 * @internal
 */
export type HeadObjectCommandOutput<K extends string> = s3.HeadObjectCommandOutput & {
  readonly __brand: 'HeadObjectCommandOutput';
  readonly __key: K;
};

/**
 * Helper functions for HeadObjectCommandOutput.
 * @internal
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
 * @internal
 */
export type ObjectVersion<K extends string> = s3.ObjectVersion & {
  readonly __brand: 'ObjectVersion';
  readonly __key: K;
};

/**
 * Helper functions for ObjectVersion.
 * @internal
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

/**
 * Branded S3 client wrapper returning nominally-typed responses.
 * @internal
 */
export class BrandedS3Client {
  constructor(private s3: s3.S3Client) {}

  async getObject<K extends string>(
    params: s3.GetObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<GetObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.GetObjectCommand(params));
    return brandWithKey<s3.GetObjectCommandOutput, 'GetObjectCommandOutput', K>(response);
  }

  async putObject<K extends string>(
    params: s3.PutObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<PutObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.PutObjectCommand(params));
    return brandWithKey<s3.PutObjectCommandOutput, 'PutObjectCommandOutput', K>(response);
  }

  async headObject<K extends string>(
    params: s3.HeadObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<HeadObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.HeadObjectCommand(params));
    return brandWithKey<s3.HeadObjectCommandOutput, 'HeadObjectCommandOutput', K>(response);
  }

  async listObjectVersions<K extends string>(
    params: s3.ListObjectVersionsCommandInput & {
      Prefix: S3KeyPath<K>;
      KeyMarker?: S3KeyPath<K>;
    }
  ): Promise<{
    Versions?: ObjectVersion<K>[];
    IsTruncated?: boolean;
    NextKeyMarker?: string;
    NextVersionIdMarker?: string;
  }> {
    const response = await this.s3.send(new s3.ListObjectVersionsCommand(params));
    return {
      ...response,
      Versions: response.Versions?.map(v => brandWithKey<s3.ObjectVersion, 'ObjectVersion', K>(v)),
    };
  }

  // Direct access to underlying S3Client for operations not wrapped
  get client(): s3.S3Client {
    return this.s3;
  }
}
