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
