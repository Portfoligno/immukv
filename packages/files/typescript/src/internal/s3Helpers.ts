/**
 * Helper functions for S3 file operations.
 *
 * These functions are not part of the public API and should only be used internally.
 */

import * as s3 from "@aws-sdk/client-s3";
import type { StreamingBlobPayloadOutputTypes } from "@smithy/types";
import type { FileVersionId, FileS3Key } from "../types";
import { brandWithKey } from "./types";

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
export function assertAwsFieldPresent<T>(
  value: T | undefined,
  fieldName: string,
): T {
  if (value === undefined) {
    throw new Error(
      `AWS SDK type bug: ${fieldName} is undefined but should always be present. ` +
        "This may indicate an AWS API change or SDK bug.",
    );
  }
  return value;
}

/**
 * PutObjectCommandOutput with corrected field optionality for file operations.
 */
export type FilePutObjectCommandOutput<K extends string> = {
  readonly __brand: "FilePutObjectCommandOutput";
  readonly __key: K;
  ETag: string;
  VersionId: string | undefined;
};

/**
 * Helper functions for file PutObjectCommandOutput.
 */
export const FilePutObjectCommandOutputs = {
  /**
   * Convert AWS SDK PutObjectCommandOutput to our FilePutObjectCommandOutput type.
   */
  fromAwsSdk: <K extends string>(
    response: s3.PutObjectCommandOutput,
  ): FilePutObjectCommandOutput<K> => {
    return brandWithKey<
      {
        ETag: string;
        VersionId: string | undefined;
      },
      "FilePutObjectCommandOutput",
      K
    >({
      ETag: assertAwsFieldPresent(response.ETag, "PutObjectCommandOutput.ETag"),
      VersionId: response.VersionId,
    });
  },

  /**
   * Extract FileVersionId from PutObjectCommandOutput.
   */
  fileVersionId: <K extends string>(
    response: FilePutObjectCommandOutput<K>,
  ): FileVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, "FileVersionId", K>(response.VersionId)
      : undefined;
  },
} as const;

/**
 * DeleteObjectCommandOutput with corrected field optionality for file operations.
 *
 * When deleting from a versioned bucket, S3 creates a "delete marker" instead of
 * actually removing the object. The VersionId in the response is the delete marker's
 * version ID, not the original object's version ID.
 */
export type FileDeleteObjectCommandOutput<K extends string> = {
  readonly __brand: "FileDeleteObjectCommandOutput";
  readonly __key: K;
  DeleteMarker: boolean | undefined;
  VersionId: string | undefined;
};

/**
 * Helper functions for file DeleteObjectCommandOutput.
 */
export const FileDeleteObjectCommandOutputs = {
  /**
   * Convert AWS SDK DeleteObjectCommandOutput to our FileDeleteObjectCommandOutput type.
   */
  fromAwsSdk: <K extends string>(
    response: s3.DeleteObjectCommandOutput,
  ): FileDeleteObjectCommandOutput<K> => {
    return brandWithKey<
      {
        DeleteMarker: boolean | undefined;
        VersionId: string | undefined;
      },
      "FileDeleteObjectCommandOutput",
      K
    >({
      DeleteMarker: response.DeleteMarker,
      VersionId: response.VersionId,
    });
  },

  /**
   * Extract the delete marker's version ID from DeleteObjectCommandOutput.
   *
   * When deleting from a versioned bucket without specifying a version ID,
   * S3 creates a delete marker. The returned VersionId is the delete marker's
   * version ID, which can be used to reference this deletion event.
   */
  deleteMarkerVersionId: <K extends string>(
    response: FileDeleteObjectCommandOutput<K>,
  ): FileVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, "FileVersionId", K>(response.VersionId)
      : undefined;
  },
} as const;

/**
 * GetObjectCommandOutput with corrected field optionality for file operations.
 */
export type FileGetObjectCommandOutput<K extends string> = {
  readonly __brand: "FileGetObjectCommandOutput";
  readonly __key: K;
  Body: StreamingBlobPayloadOutputTypes;
  ETag: string;
  VersionId: string | undefined;
  ContentLength: number;
  ContentType: string | undefined;
  Metadata: Record<string, string> | undefined;
};

/**
 * Helper functions for file GetObjectCommandOutput.
 */
export const FileGetObjectCommandOutputs = {
  /**
   * Convert AWS SDK GetObjectCommandOutput to our FileGetObjectCommandOutput type.
   */
  fromAwsSdk: <K extends string>(
    response: s3.GetObjectCommandOutput,
  ): FileGetObjectCommandOutput<K> => {
    return brandWithKey<
      {
        Body: StreamingBlobPayloadOutputTypes;
        ETag: string;
        VersionId: string | undefined;
        ContentLength: number;
        ContentType: string | undefined;
        Metadata: Record<string, string> | undefined;
      },
      "FileGetObjectCommandOutput",
      K
    >({
      Body: assertAwsFieldPresent(response.Body, "GetObjectCommandOutput.Body"),
      ETag: assertAwsFieldPresent(response.ETag, "GetObjectCommandOutput.ETag"),
      VersionId: response.VersionId,
      ContentLength: assertAwsFieldPresent(
        response.ContentLength,
        "GetObjectCommandOutput.ContentLength",
      ),
      ContentType: response.ContentType,
      Metadata: response.Metadata,
    });
  },

  /**
   * Extract FileVersionId from GetObjectCommandOutput.
   */
  fileVersionId: <K extends string>(
    response: FileGetObjectCommandOutput<K>,
  ): FileVersionId<K> | undefined => {
    return response.VersionId !== undefined
      ? brandWithKey<string, "FileVersionId", K>(response.VersionId)
      : undefined;
  },
} as const;

/**
 * Branded S3 client wrapper for file operations.
 */
export class FileS3Client {
  constructor(private s3Client: s3.S3Client) {}

  /**
   * Upload file to S3.
   */
  async putObject<K extends string>(params: {
    Bucket: string;
    Key: FileS3Key<K>;
    Body: Buffer | string;
    ContentType?: string;
    Metadata?: Record<string, string>;
    SSEKMSKeyId?: string;
    ServerSideEncryption?: s3.ServerSideEncryption;
  }): Promise<FilePutObjectCommandOutput<K>> {
    const command = new s3.PutObjectCommand({
      Bucket: params.Bucket,
      Key: params.Key,
      Body: params.Body,
      ContentType: params.ContentType ?? "application/octet-stream",
      Metadata: params.Metadata,
      SSEKMSKeyId: params.SSEKMSKeyId,
      ServerSideEncryption: params.ServerSideEncryption,
    });
    const response = await this.s3Client.send(command);
    return FilePutObjectCommandOutputs.fromAwsSdk(response);
  }

  /**
   * Download file from S3.
   */
  async getObject<K extends string>(params: {
    Bucket: string;
    Key: FileS3Key<K>;
    VersionId?: FileVersionId<K>;
  }): Promise<FileGetObjectCommandOutput<K>> {
    const command = new s3.GetObjectCommand({
      Bucket: params.Bucket,
      Key: params.Key,
      VersionId: params.VersionId,
    });
    const response = await this.s3Client.send(command);
    return FileGetObjectCommandOutputs.fromAwsSdk(response);
  }

  /**
   * Delete file from S3.
   *
   * In a versioned bucket, this creates a delete marker rather than
   * permanently removing the object. The returned VersionId is the
   * delete marker's version ID.
   */
  async deleteObject<K extends string>(params: {
    Bucket: string;
    Key: FileS3Key<K>;
  }): Promise<FileDeleteObjectCommandOutput<K>> {
    const command = new s3.DeleteObjectCommand({
      Bucket: params.Bucket,
      Key: params.Key,
    });
    const response = await this.s3Client.send(command);
    return FileDeleteObjectCommandOutputs.fromAwsSdk(response);
  }

  /**
   * Check bucket versioning status.
   */
  async getBucketVersioning(
    bucket: string,
  ): Promise<s3.GetBucketVersioningCommandOutput> {
    const command = new s3.GetBucketVersioningCommand({ Bucket: bucket });
    return await this.s3Client.send(command);
  }

  /**
   * Head request to check bucket access.
   */
  async headBucket(bucket: string): Promise<void> {
    const command = new s3.HeadBucketCommand({ Bucket: bucket });
    await this.s3Client.send(command);
  }

  /**
   * Direct access to underlying S3Client for operations not wrapped.
   */
  get client(): s3.S3Client {
    return this.s3Client;
  }
}
