/**
 * Branded S3 client wrapper for type-safe operations.
 *
 * This client is not part of the public API and should only be used internally.
 */

import * as s3 from '@aws-sdk/client-s3';
import {
  S3KeyPath,
  GetObjectCommandOutput,
  GetObjectCommandOutputs,
  PutObjectCommandOutput,
  PutObjectCommandOutputs,
  HeadObjectCommandOutput,
  HeadObjectCommandOutputs,
  ListObjectVersionsCommandOutput,
  ListObjectVersionsCommandOutputs,
  ListObjectsV2CommandOutput,
  ListObjectsV2CommandOutputs,
} from './s3Types';

/**
 * Returns a FetchHttpHandler configured with `cache: "no-cache"` when running
 * in a browser environment, or undefined in Node.js (letting the SDK use its
 * default NodeHttpHandler).
 *
 * Browser fetch() uses heuristic freshness caching (RFC 7234) by default,
 * which can serve stale S3 responses during the retry loop in set().
 * Setting `cache: "no-cache"` in RequestInit forces the browser to always
 * revalidate with the server, while still allowing 304 Not Modified responses
 * when the ETag matches (preserving bandwidth savings for unchanged data).
 *
 * This is a RequestInit property (not an HTTP header), so it has zero impact
 * on SigV4 signing.
 */
export function createBrowserSafeRequestHandler(): s3.S3ClientConfig['requestHandler'] {
  // Detect browser environment via typeof checks (safe even without DOM types)
  const isBrowser =
    typeof globalThis !== 'undefined' &&
    typeof (globalThis as Record<string, unknown>)['document'] !== 'undefined';

  if (isBrowser) {
    // Dynamically require to avoid bundling issues in Node.js.
    // @smithy/fetch-http-handler is a transitive dependency of @aws-sdk/client-s3
    // and is always available in browser builds.
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
      const { FetchHttpHandler } = require('@smithy/fetch-http-handler');
      return new FetchHttpHandler({ requestInit: { cache: 'no-cache' } });
    } catch {
      // If the import fails (e.g. in a non-standard bundler environment),
      // fall through to undefined — the SDK will use its default handler.
    }
  }
  return undefined;
}

/**
 * Branded S3 client wrapper returning nominally-typed responses.
 */
export class BrandedS3Client {
  constructor(private s3: s3.S3Client) {}

  async getObject<K extends string>(
    params: s3.GetObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<GetObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.GetObjectCommand(params));
    return GetObjectCommandOutputs.fromAwsSdk(response);
  }

  async putObject<K extends string>(
    params: s3.PutObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<PutObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.PutObjectCommand(params));
    return PutObjectCommandOutputs.fromAwsSdk(response);
  }

  async headObject<K extends string>(
    params: s3.HeadObjectCommandInput & { Key: S3KeyPath<K> }
  ): Promise<HeadObjectCommandOutput<K>> {
    const response = await this.s3.send(new s3.HeadObjectCommand(params));
    return HeadObjectCommandOutputs.fromAwsSdk(response);
  }

  async listObjectVersions<K extends string>(
    params: s3.ListObjectVersionsCommandInput & {
      Prefix: S3KeyPath<K>;
      KeyMarker?: S3KeyPath<K>;
    }
  ): Promise<ListObjectVersionsCommandOutput<K>> {
    const response = await this.s3.send(new s3.ListObjectVersionsCommand(params));
    return ListObjectVersionsCommandOutputs.fromAwsSdk(response);
  }

  async listObjectsV2(params: s3.ListObjectsV2CommandInput): Promise<ListObjectsV2CommandOutput> {
    const response = await this.s3.send(new s3.ListObjectsV2Command(params));
    return ListObjectsV2CommandOutputs.fromAwsSdk(response);
  }

  // Direct access to underlying S3Client for operations not wrapped
  get client(): s3.S3Client {
    return this.s3;
  }
}
