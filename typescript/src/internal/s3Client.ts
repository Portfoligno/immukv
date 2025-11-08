/**
 * Branded S3 client wrapper for type-safe operations.
 *
 * This client is not part of the public API and should only be used internally.
 */

import * as s3 from '@aws-sdk/client-s3';
import { brandWithKey } from '../types';
import {
  S3KeyPath,
  GetObjectCommandOutput,
  PutObjectCommandOutput,
  HeadObjectCommandOutput,
  ObjectVersion,
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
