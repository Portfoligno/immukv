/**
 * FileClient implementation for ImmuKV file storage.
 */

import * as s3 from "@aws-sdk/client-s3";
import type { Readable } from "stream";
import type {
  ImmuKVClient,
  Config as ImmuKVConfig,
  KeyVersionId,
  JSONValue,
} from "immukv";
import type {
  ContentHash,
  FileS3Key,
  FileVersionId,
  FileStorageConfig,
  FileMetadata,
  DeletedFileMetadata,
  FileValue,
  FileEntry,
  SetFileOptions,
  GetFileOptions,
  FileDownload,
} from "./types";
import {
  isDeletedFile,
  FileNotFoundError,
  FileDeletedError,
  ConfigurationError,
  MaxRetriesExceededError,
} from "./types";
import { FileS3Keys, contentHashFromJson } from "./internal/types";
import type { UploadResult } from "./internal/types";
import {
  computeHashFromBuffer,
  computeHashFromString,
  computeHashFromStream,
} from "./internal/hashing";
import {
  FileS3Client,
  FilePutObjectCommandOutputs,
  FileDeleteObjectCommandOutputs,
} from "./internal/s3Helpers";
import * as fs from "fs";
import * as path from "path";

/**
 * FileClient for storing and retrieving files with ImmuKV audit logging.
 *
 * Uses three-phase write protocol:
 * 1. Upload file to S3 (compute hash during upload)
 * 2. Write log entry to ImmuKV (commit point)
 * 3. Write key object for fast lookup (best effort)
 */
export class FileClient<K extends string = string> {
  /**
   * The underlying ImmuKV client for log operations.
   */
  readonly kvClient: ImmuKVClient<K, FileValue<K>>;

  private readonly fileS3: FileS3Client;
  private readonly ownsS3Client: boolean;
  private readonly fileBucket: string;
  private readonly filePrefix: string;
  private readonly kmsKeyId?: string;

  /**
   * Create a FileClient.
   *
   * For most use cases, prefer the static `create()` factory method
   * which validates bucket access and versioning.
   *
   * @param kvClient - ImmuKV client for log operations
   * @param config - Optional file storage configuration
   */
  constructor(
    kvClient: ImmuKVClient<K, FileValue<K>>,
    config?: FileStorageConfig,
  ) {
    this.kvClient = kvClient;

    // Access internal config from kvClient
    const kvConfig = (kvClient as any).config as ImmuKVConfig;

    // Determine S3 client sharing
    const sameRegion =
      (config?.region ?? kvConfig.s3Region) === kvConfig.s3Region;
    const sameOverrides = config?.overrides === undefined;
    const sameBucket = config?.bucket === undefined;

    if (sameBucket && sameRegion && sameOverrides) {
      // Share S3 client from kvClient
      this.fileS3 = new FileS3Client(
        (kvClient as any).s3.client as s3.S3Client,
      );
      this.ownsS3Client = false;
    } else {
      // Create separate S3 client
      this.fileS3 = new FileS3Client(
        new s3.S3Client({
          region: config?.region ?? kvConfig.s3Region,
          endpoint: config?.overrides?.endpointUrl,
          credentials: config?.overrides?.credentials,
          forcePathStyle: config?.overrides?.forcePathStyle,
        }),
      );
      this.ownsS3Client = true;
    }

    // Determine file bucket and prefix
    this.fileBucket = config?.bucket ?? kvConfig.s3Bucket;
    if (config?.bucket !== undefined) {
      // Different bucket: default to no prefix
      this.filePrefix = config.prefix ?? "";
    } else {
      // Same bucket: default to "files/" prefix under kvClient prefix
      this.filePrefix = config?.prefix ?? `${kvConfig.s3Prefix}files/`;
    }

    this.kmsKeyId = config?.kmsKeyId;
  }

  /**
   * Create a FileClient with validation.
   *
   * Validates bucket access and versioning status before returning.
   * This is the recommended way to create a FileClient.
   *
   * @param kvClient - ImmuKV client for log operations
   * @param config - Optional file storage configuration
   * @returns Promise resolving to validated FileClient
   * @throws ConfigurationError if bucket is inaccessible or versioning disabled
   */
  static async create<K extends string>(
    kvClient: ImmuKVClient<K, FileValue<K>>,
    config?: FileStorageConfig,
  ): Promise<FileClient<K>> {
    const client = new FileClient<K>(kvClient, config);

    const validateAccess = config?.validateAccess ?? true;
    const validateVersioning = config?.validateVersioning ?? true;

    if (validateAccess) {
      try {
        await client.fileS3.headBucket(client.fileBucket);
      } catch (error: any) {
        throw new ConfigurationError(
          `Cannot access file bucket '${client.fileBucket}': ${error.message}`,
        );
      }
    }

    if (validateVersioning) {
      await client.validateVersioning();
    }

    return client;
  }

  /**
   * Upload a file and record it in the log.
   *
   * Three-phase write protocol:
   * 1. Upload file to S3 (compute hash during upload)
   * 2. Write log entry to ImmuKV (commit point)
   * 3. Write key object for fast lookup (best effort)
   *
   * @param key - User-supplied file key
   * @param source - File content as stream, buffer, or string path
   * @param options - Optional settings (contentType, userMetadata)
   * @returns Promise resolving to the file entry
   * @throws MaxRetriesExceededError if log write fails after retries
   */
  async setFile(
    key: K,
    source: Readable | Buffer | string,
    options?: SetFileOptions,
  ): Promise<FileEntry<K>> {
    const maxRetries = 10;
    let uploadResult: UploadResult<K> | undefined;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      // Phase 1: Upload file only once (cache result for retries)
      if (uploadResult === undefined) {
        uploadResult = await this.uploadFile(key, source, options);
      }

      try {
        // Phase 2: Write log entry (may conflict on concurrent writes)
        const metadata: FileMetadata<K> = {
          s3Key: uploadResult.s3Key,
          s3VersionId: uploadResult.s3VersionId,
          contentHash: uploadResult.contentHash,
          contentLength: uploadResult.contentLength,
          contentType: uploadResult.contentType,
          userMetadata: uploadResult.userMetadata,
        };

        // Use kvClient.set() which handles log write with optimistic locking
        const entry = await this.kvClient.set(key, metadata as FileValue<K>);

        // Phase 3: Key object write is handled by kvClient.set() internally

        return entry;
      } catch (error: any) {
        // Retry on precondition failure (concurrent write conflict)
        if (
          error.name === "PreconditionFailed" ||
          error.$metadata?.httpStatusCode === 412
        ) {
          // DO NOT re-upload - reuse cached uploadResult
          continue;
        }
        throw error;
      }
    }

    throw new MaxRetriesExceededError(
      `Failed to write file entry for '${key}' after ${maxRetries} retries`,
    );
  }

  /**
   * Get a file by key.
   *
   * Returns the file content as a stream along with metadata.
   * Supports historical access via versionId option.
   *
   * @param key - File key
   * @param options - Optional settings (versionId for historical access)
   * @returns Promise resolving to file download with entry and stream
   * @throws FileNotFoundError if key does not exist
   * @throws FileDeletedError if file has been deleted
   */
  async getFile(key: K, options?: GetFileOptions<K>): Promise<FileDownload<K>> {
    // Get entry from log
    let entry: FileEntry<K>;

    if (options?.versionId !== undefined) {
      // Historical access via specific version
      const [history] = await this.kvClient.history(key, options.versionId, 1);
      if (history.length === 0) {
        throw new FileNotFoundError(
          `File '${key}' version '${options.versionId}' not found`,
        );
      }
      entry = history[0];
    } else {
      // Current version
      try {
        entry = await this.kvClient.get(key);
      } catch (error: any) {
        if (error.name === "KeyNotFoundError") {
          // Fallback: check log history (key object might be missing)
          const [history] = await this.kvClient.history(key, undefined, 1);
          if (history.length === 0) {
            throw new FileNotFoundError(`File '${key}' not found`);
          }
          entry = history[0];
        } else {
          throw error;
        }
      }
    }

    // Check if deleted
    if (isDeletedFile(entry.value)) {
      throw new FileDeletedError(`File '${key}' has been deleted`);
    }

    // Download file from S3
    const fileValue = entry.value as FileMetadata<K>;
    const response = await this.fileS3.getObject({
      Bucket: this.fileBucket,
      Key: fileValue.s3Key,
      VersionId: fileValue.s3VersionId,
    });

    return {
      entry,
      stream: response.Body as unknown as NodeJS.ReadableStream,
    };
  }

  /**
   * Get a file and write it to a local path.
   *
   * @param key - File key
   * @param destPath - Destination file path
   * @param options - Optional settings (versionId for historical access)
   * @returns Promise resolving to the file entry
   * @throws FileNotFoundError if key does not exist
   * @throws FileDeletedError if file has been deleted
   */
  async getFileToPath(
    key: K,
    destPath: string,
    options?: GetFileOptions<K>,
  ): Promise<FileEntry<K>> {
    const { entry, stream } = await this.getFile(key, options);

    // Ensure directory exists
    const dir = path.dirname(destPath);
    await fs.promises.mkdir(dir, { recursive: true });

    // Write stream to file
    const writeStream = fs.createWriteStream(destPath);
    await new Promise<void>((resolve, reject) => {
      (stream as Readable).pipe(writeStream);
      writeStream.on("finish", resolve);
      writeStream.on("error", reject);
      (stream as Readable).on("error", reject);
    });

    return entry;
  }

  /**
   * Delete a file.
   *
   * Three-phase delete protocol:
   * 1. Delete S3 object (creates delete marker in versioned bucket)
   * 2. Write tombstone entry to log with delete marker's version ID
   * 3. Update key object (handled by kvClient.set)
   *
   * The original file content remains accessible via S3 versioning for audit purposes.
   * Use history() to see deleted files and their original content hashes.
   *
   * @param key - File key to delete
   * @returns Promise resolving to the tombstone entry
   * @throws FileNotFoundError if key does not exist
   * @throws FileDeletedError if file is already deleted
   * @throws ConfigurationError if S3 delete does not return a version ID
   */
  async deleteFile(key: K): Promise<FileEntry<K>> {
    // Get current entry to verify it exists and is not deleted
    let currentEntry: FileEntry<K>;
    try {
      currentEntry = await this.kvClient.get(key);
    } catch (error: any) {
      if (error.name === "KeyNotFoundError") {
        throw new FileNotFoundError(`File '${key}' not found`);
      }
      throw error;
    }

    if (isDeletedFile(currentEntry.value)) {
      throw new FileDeletedError(`File '${key}' is already deleted`);
    }

    const currentValue = currentEntry.value as FileMetadata<K>;

    // Phase 1: Delete S3 object (creates delete marker in versioned bucket)
    const deleteResponse = await this.fileS3.deleteObject({
      Bucket: this.fileBucket,
      Key: currentValue.s3Key,
    });

    const deleteMarkerVersionId =
      FileDeleteObjectCommandOutputs.deleteMarkerVersionId(deleteResponse);
    if (deleteMarkerVersionId === undefined) {
      throw new ConfigurationError(
        `S3 DeleteObject response missing VersionId - ensure versioning is enabled on bucket '${this.fileBucket}'`,
      );
    }

    // Phase 2: Write tombstone entry with delete marker's version ID
    const tombstone: DeletedFileMetadata<K> = {
      deleted: true,
      s3Key: currentValue.s3Key,
      deletedVersionId: deleteMarkerVersionId,
    };

    // Phase 3: Key object update is handled by kvClient.set internally
    return await this.kvClient.set(key, tombstone as FileValue<K>);
  }

  /**
   * Get history of a file key.
   *
   * Returns all entries including deletions, newest first.
   *
   * @param key - File key
   * @param beforeVersionId - Pagination cursor (pass last versionId from previous result)
   * @param limit - Maximum entries to return
   * @returns Promise resolving to [entries, nextCursor]
   */
  async history(
    key: K,
    beforeVersionId?: KeyVersionId<K>,
    limit?: number,
  ): Promise<[FileEntry<K>[], KeyVersionId<K> | undefined]> {
    return await this.kvClient.history(key, beforeVersionId, limit);
  }

  /**
   * List file keys.
   *
   * Returns keys in lexicographic order.
   * Pass the last key from the previous result as `afterKey` for pagination.
   *
   * @param afterKey - Pagination cursor
   * @param limit - Maximum keys to return
   * @returns Promise resolving to array of keys
   */
  async listFiles(afterKey?: K, limit?: number): Promise<K[]> {
    return await this.kvClient.listKeys(afterKey, limit);
  }

  /**
   * Verify file integrity by downloading and checking content hash.
   *
   * For active files: downloads file and verifies contentHash.
   * For tombstones: verifies entry hash only (no content to verify).
   *
   * @param entry - File entry to verify
   * @returns Promise resolving to true if integrity check passes
   */
  async verifyFile(entry: FileEntry<K>): Promise<boolean> {
    // First verify entry hash (metadata integrity)
    const entryValid = await this.kvClient.verify(entry);
    if (!entryValid) {
      return false;
    }

    // For tombstones, entry hash verification is sufficient
    if (isDeletedFile(entry.value)) {
      return true;
    }

    // For active files, download and verify content hash
    const fileValue = entry.value as FileMetadata<K>;

    try {
      const response = await this.fileS3.getObject({
        Bucket: this.fileBucket,
        Key: fileValue.s3Key,
        VersionId: fileValue.s3VersionId,
      });

      // Stream to buffer and compute hash
      const body = response.Body;
      const chunks: Buffer[] = [];
      for await (const chunk of body as AsyncIterable<Buffer>) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      const buffer = Buffer.concat(chunks);

      const actualHash = computeHashFromBuffer<K>(buffer);
      return actualHash === fileValue.contentHash;
    } catch (error: any) {
      if (error.name === "NoSuchKey" || error.name === "NoSuchVersion") {
        // File missing from S3
        return false;
      }
      throw error;
    }
  }

  /**
   * Close the client and cleanup resources.
   *
   * Only destroys S3 client if it was created by this FileClient.
   * The underlying kvClient is not closed - caller is responsible for that.
   */
  async close(): Promise<void> {
    if (this.ownsS3Client) {
      this.fileS3.client.destroy();
    }
  }

  // Private helper methods

  /**
   * Upload file to S3 and compute hash.
   */
  private async uploadFile(
    key: K,
    source: Readable | Buffer | string,
    options?: SetFileOptions,
  ): Promise<UploadResult<K>> {
    const s3Key = FileS3Keys.forFile(this.filePrefix, key);
    const contentType = options?.contentType ?? "application/octet-stream";

    // Prepare content and compute hash
    let buffer: Buffer;
    let contentHash: ContentHash<K>;
    let contentLength: number;

    if (Buffer.isBuffer(source)) {
      buffer = source;
      contentHash = computeHashFromBuffer<K>(source);
      contentLength = source.length;
    } else if (typeof source === "string") {
      // Check if it's a file path
      if (fs.existsSync(source)) {
        buffer = await fs.promises.readFile(source);
        contentHash = computeHashFromBuffer<K>(buffer);
        contentLength = buffer.length;
      } else {
        // Treat as string content
        buffer = Buffer.from(source, "utf-8");
        contentHash = computeHashFromString<K>(source);
        contentLength = buffer.length;
      }
    } else {
      // Readable stream - buffer and hash
      const result = await computeHashFromStream<K>(source);
      buffer = result.buffer;
      contentHash = result.contentHash;
      contentLength = result.contentLength;
    }

    // Upload to S3
    const putResponse = await this.fileS3.putObject({
      Bucket: this.fileBucket,
      Key: s3Key,
      Body: buffer,
      ContentType: contentType,
      Metadata: options?.userMetadata,
      SSEKMSKeyId: this.kmsKeyId,
      ServerSideEncryption: this.kmsKeyId !== undefined ? "aws:kms" : undefined,
    });

    const s3VersionId = FilePutObjectCommandOutputs.fileVersionId(putResponse);
    if (s3VersionId === undefined) {
      throw new ConfigurationError(
        `S3 PutObject response missing VersionId - ensure versioning is enabled on bucket '${this.fileBucket}'`,
      );
    }

    return {
      s3Key,
      s3VersionId,
      contentHash,
      contentLength,
      contentType,
      userMetadata: options?.userMetadata,
    };
  }

  /**
   * Validate that file bucket has versioning enabled.
   */
  private async validateVersioning(): Promise<void> {
    const response = await this.fileS3.getBucketVersioning(this.fileBucket);

    if (response.Status !== "Enabled") {
      throw new ConfigurationError(
        `File bucket '${this.fileBucket}' must have versioning enabled. ` +
          `Current: ${response.Status ?? "Disabled"}. ` +
          `Enable with: aws s3api put-bucket-versioning --bucket ${this.fileBucket} ` +
          `--versioning-configuration Status=Enabled`,
      );
    }
  }
}

/**
 * Value decoder for FileValue JSON.
 *
 * Used internally to parse file metadata from ImmuKV entries.
 */
export function fileValueDecoder<K extends string>(
  json: JSONValue,
): FileValue<K> {
  const obj = json as Record<string, JSONValue>;

  if (obj.deleted === true) {
    // Tombstone
    return {
      deleted: true,
      s3Key: obj.s3Key as FileS3Key<K>,
      deletedVersionId: obj.deletedVersionId as FileVersionId<K>,
    } as DeletedFileMetadata<K>;
  }

  // Active file
  return {
    s3Key: obj.s3Key as FileS3Key<K>,
    s3VersionId: obj.s3VersionId as FileVersionId<K>,
    contentHash: contentHashFromJson<K>(obj.contentHash as string),
    contentLength: obj.contentLength as number,
    contentType: obj.contentType as string,
    userMetadata: obj.userMetadata as Record<string, string> | undefined,
  } as FileMetadata<K>;
}

/**
 * Value encoder for FileValue to JSON.
 *
 * Used internally to serialize file metadata for ImmuKV entries.
 */
export function fileValueEncoder<K extends string>(
  value: FileValue<K>,
): JSONValue {
  if (isDeletedFile(value)) {
    return {
      deleted: true,
      s3Key: value.s3Key,
      deletedVersionId: value.deletedVersionId,
    };
  }

  const result: Record<string, JSONValue> = {
    s3Key: value.s3Key,
    s3VersionId: value.s3VersionId,
    contentHash: value.contentHash,
    contentLength: value.contentLength,
    contentType: value.contentType,
  };

  if (value.userMetadata !== undefined) {
    result.userMetadata = value.userMetadata;
  }

  return result;
}

/**
 * Create a FileClient from an ImmuKV client and configuration.
 *
 * This is the recommended way to create a FileClient.
 * Validates bucket access and versioning status.
 *
 * @param kvClient - ImmuKV client (will be used with file codecs)
 * @param config - Optional file storage configuration
 * @returns Promise resolving to validated FileClient
 */
export async function createFileClient<K extends string>(
  kvClient: ImmuKVClient<K, any>,
  config?: FileStorageConfig,
): Promise<FileClient<K>> {
  // Create typed client with file codecs
  const typedClient = kvClient.withCodec<K, FileValue<K>>(
    fileValueDecoder,
    fileValueEncoder,
  );
  return await FileClient.create(typedClient, config);
}
