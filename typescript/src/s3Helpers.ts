/**
 * Helper functions for S3 operations.
 *
 * Centralizes type handling for AWS SDK responses to maintain type safety.
 */

import { JSONValue } from './jsonHelpers';
import { LogVersionId, KeyVersionId, KeyObjectETag } from './types';

/**
 * S3 Body type from AWS SDK GetObject response.
 * The Body can be a stream that needs to be read.
 */
type S3Body =
  | {
      transformToString: (encoding?: string) => Promise<string>;
    }
  | undefined;

/**
 * Read S3 Body stream and parse as JSON.
 *
 * Centralizes JSON parsing from S3 GetObject responses.
 *
 * @param body - S3 response body stream
 * @returns Parsed JSON object
 * @throws Error if body is undefined or JSON parsing fails
 */
export async function readBodyAsJson(body: S3Body): Promise<{ [key: string]: JSONValue }> {
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
 */
export function getErrorCode(error: any): string {
  return error.name || error.$metadata?.httpStatusCode?.toString() || 'Unknown';
}

// S3 response types (simplified - only fields we use)

interface S3GetObjectResponse {
  VersionId?: string;
  ETag?: string;
  Body?: S3Body;
}

interface S3PutObjectResponse {
  VersionId?: string;
  ETag?: string;
}

interface S3HeadObjectResponse {
  VersionId?: string;
  ETag?: string;
}

interface S3ObjectVersion {
  VersionId?: string;
  Key?: string;
  ETag?: string;
}

// S3 response field extraction helpers

/**
 * Extract LogVersionId from GetObject response.
 */
export function logVersionIdFromGet<K extends string>(
  response: S3GetObjectResponse
): LogVersionId<K> {
  return response.VersionId as LogVersionId<K>;
}

/**
 * Extract LogVersionId from PutObject response.
 */
export function logVersionIdFromPut<K extends string>(
  response: S3PutObjectResponse
): LogVersionId<K> {
  return response.VersionId as LogVersionId<K>;
}

/**
 * Extract LogVersionId from HeadObject response.
 */
export function logVersionIdFromHead<K extends string>(
  response: S3HeadObjectResponse
): LogVersionId<K> {
  return response.VersionId as LogVersionId<K>;
}

/**
 * Extract LogVersionId from S3ObjectVersion.
 */
export function logVersionIdFromVersion<K extends string>(
  version: S3ObjectVersion
): LogVersionId<K> {
  return version.VersionId as LogVersionId<K>;
}

/**
 * Extract KeyVersionId from S3ObjectVersion.
 */
export function keyVersionIdFromVersion<K extends string>(
  version: S3ObjectVersion
): KeyVersionId<K> {
  return version.VersionId as KeyVersionId<K>;
}

/**
 * Extract KeyObjectETag from GetObject response.
 */
export function keyObjectEtagFromGet<K extends string>(
  response: S3GetObjectResponse
): KeyObjectETag<K> {
  return response.ETag as KeyObjectETag<K>;
}

/**
 * Extract KeyObjectETag from PutObject response.
 */
export function keyObjectEtagFromPut<K extends string>(
  response: S3PutObjectResponse
): KeyObjectETag<K> {
  return response.ETag as KeyObjectETag<K>;
}

/**
 * Extract KeyObjectETag from HeadObject response.
 */
export function keyObjectEtagFromHead<K extends string>(
  response: S3HeadObjectResponse
): KeyObjectETag<K> {
  return response.ETag as KeyObjectETag<K>;
}
