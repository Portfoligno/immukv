/**
 * Helper functions for S3 operations.
 *
 * These functions are not part of the public API and should only be used internally.
 */

import type { StreamingBlobPayloadOutputTypes } from '@smithy/types';
import { JSONValue } from '../jsonHelpers';

/**
 * Read S3 Body stream and parse as JSON.
 *
 * Centralizes JSON parsing from S3 GetObject responses.
 *
 * @param body - S3 response body stream
 * @returns Parsed JSON object
 * @throws Error if JSON parsing fails
 */
export async function readBodyAsJson(
  body: StreamingBlobPayloadOutputTypes
): Promise<{ [key: string]: JSONValue }> {
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
  return error.name ?? error.$metadata?.httpStatusCode?.toString() ?? 'Unknown';
}

/**
 * Assert that a field marked optional by AWS SDK types is actually present.
 *
 * The AWS SDK TypeScript types incorrectly mark many fields as optional due to
 * Smithy type generation bugs (see GitHub issue #5992). This helper asserts
 * that fields which are always returned by AWS are actually present at runtime.
 *
 * @param value - The supposedly optional value
 * @param fieldName - Name of the field for error messages
 * @returns The value with undefined removed from type
 * @throws Error if value is undefined (indicates AWS SDK bug or API change)
 */
export function assertAwsFieldPresent<T>(value: T | undefined, fieldName: string): T {
  if (value === undefined) {
    throw new Error(
      `AWS SDK type bug: ${fieldName} is undefined but should always be present. ` +
        'This may indicate an AWS API change or SDK bug.'
    );
  }
  return value;
}
