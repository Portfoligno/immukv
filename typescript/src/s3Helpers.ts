/**
 * Helper functions for S3 operations.
 *
 * Centralizes type handling for AWS SDK responses to maintain type safety.
 */

import { JSONValue } from './jsonHelpers';

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
