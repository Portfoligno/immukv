/**
 * SHA-256 streaming hash computation for file content.
 */

import * as crypto from "crypto";
import type { Readable } from "stream";
import type { ContentHash } from "../types";
import { contentHashFromDigest } from "./types";

/**
 * Compute SHA-256 hash of a buffer.
 *
 * @param data - Buffer to hash
 * @returns ContentHash with 'sha256:' prefix
 */
export function computeHashFromBuffer<K extends string>(
  data: Buffer,
): ContentHash<K> {
  const hash = crypto.createHash("sha256");
  hash.update(data);
  const hexDigest = hash.digest("hex");
  return contentHashFromDigest<K>(hexDigest);
}

/**
 * Compute SHA-256 hash of a string.
 *
 * @param data - String to hash (encoded as UTF-8)
 * @returns ContentHash with 'sha256:' prefix
 */
export function computeHashFromString<K extends string>(
  data: string,
): ContentHash<K> {
  return computeHashFromBuffer<K>(Buffer.from(data, "utf-8"));
}

/**
 * Result of streaming hash computation.
 * Contains both the hash and the buffered content for upload.
 */
export interface StreamHashResult<K extends string> {
  /** SHA-256 hash of the content */
  contentHash: ContentHash<K>;

  /** Buffered content for upload */
  buffer: Buffer;

  /** Content length in bytes */
  contentLength: number;
}

/**
 * Compute SHA-256 hash of a readable stream.
 *
 * Buffers the entire stream to compute hash and return content for upload.
 * For large files, consider using multipart upload with per-part hashing.
 *
 * @param stream - Readable stream to hash
 * @returns Promise resolving to hash and buffered content
 */
export async function computeHashFromStream<K extends string>(
  stream: Readable,
): Promise<StreamHashResult<K>> {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const chunks: Buffer[] = [];

    stream.on("data", (chunk: Buffer | string) => {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      hash.update(buffer);
      chunks.push(buffer);
    });

    stream.on("end", () => {
      const hexDigest = hash.digest("hex");
      const buffer = Buffer.concat(chunks);
      resolve({
        contentHash: contentHashFromDigest<K>(hexDigest),
        buffer,
        contentLength: buffer.length,
      });
    });

    stream.on("error", (error) => {
      reject(error);
    });
  });
}

/**
 * Verify that a buffer matches an expected content hash.
 *
 * @param data - Buffer to verify
 * @param expectedHash - Expected ContentHash
 * @returns true if hash matches
 */
export function verifyBufferHash<K extends string>(
  data: Buffer,
  expectedHash: ContentHash<K>,
): boolean {
  const actualHash = computeHashFromBuffer<K>(data);
  return actualHash === expectedHash;
}

/**
 * Verify that a stream matches an expected content hash.
 *
 * @param stream - Readable stream to verify
 * @param expectedHash - Expected ContentHash
 * @returns Promise resolving to true if hash matches
 */
export async function verifyStreamHash<K extends string>(
  stream: Readable,
  expectedHash: ContentHash<K>,
): Promise<boolean> {
  const result = await computeHashFromStream<K>(stream);
  return result.contentHash === expectedHash;
}
