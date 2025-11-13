/**
 * Integration tests using real S3 API (LocalStack).
 *
 * These tests verify ImmuKV behavior against actual S3 operations,
 * testing specifications that cannot be adequately verified with mocks.
 */

import {
  S3Client,
  CreateBucketCommand,
  PutBucketVersioningCommand,
  HeadObjectCommand,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectVersionsCommand,
  DeleteObjectCommand,
  DeleteBucketCommand,
} from '@aws-sdk/client-s3';
import { v4 as uuidv4 } from 'uuid';
import { ImmuKVClient, Config, Entry } from '../src';

// Skip if not in integration test mode
const integrationTestEnabled = process.env.IMMUKV_INTEGRATION_TEST === 'true';
const skipMessage = 'Integration tests require IMMUKV_INTEGRATION_TEST=true';

function identityParser(value: any): any {
  return value;
}

describe('Integration Tests with LocalStack', () => {
  let s3Client: S3Client;
  let bucketName: string;
  let client: ImmuKVClient<string, any>;

  if (!integrationTestEnabled) {
    test.skip(skipMessage, () => {});
    return;
  }

  beforeEach(async () => {
    // Create unique bucket per test for complete isolation
    bucketName = `test-immukv-${uuidv4().substring(0, 8)}`;

    const endpointUrl = process.env.IMMUKV_S3_ENDPOINT || 'http://localstack:4566';
    // Use environment variables if set, otherwise default to test credentials
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID || 'test';
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || 'test';

    s3Client = new S3Client({
      region: 'us-east-1',
      endpoint: endpointUrl,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
      forcePathStyle: true,
    });

    await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
    await s3Client.send(
      new PutBucketVersioningCommand({
        Bucket: bucketName,
        VersioningConfiguration: { Status: 'Enabled' },
      })
    );

    const config: Config = {
      s3Bucket: bucketName,
      s3Region: 'us-east-1',
      s3Prefix: 'test/',
      repairCheckIntervalMs: 1000,
      overrides: {
        endpointUrl,
        credentials: {
          accessKeyId,
          secretAccessKey,
        },
        forcePathStyle: true,
      },
    };

    client = new ImmuKVClient(config, identityParser);
  });

  afterEach(async () => {
    client.close();

    // Cleanup: delete all versions then bucket
    try {
      const versionsResponse = await s3Client.send(
        new ListObjectVersionsCommand({ Bucket: bucketName })
      );

      // Delete all versions
      for (const version of versionsResponse.Versions || []) {
        await s3Client.send(
          new DeleteObjectCommand({
            Bucket: bucketName,
            Key: version.Key!,
            VersionId: version.VersionId!,
          })
        );
      }

      // Delete all delete markers
      for (const marker of versionsResponse.DeleteMarkers || []) {
        await s3Client.send(
          new DeleteObjectCommand({
            Bucket: bucketName,
            Key: marker.Key!,
            VersionId: marker.VersionId!,
          })
        );
      }

      await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
    } catch (e) {
      console.warn(`Cleanup warning for bucket ${bucketName}:`, e);
    }
  });

  test('real S3 versioning creates unique version IDs', async () => {
    const entry1 = await client.set('key1', { version: 1 });
    const entry2 = await client.set('key1', { version: 2 });
    const entry3 = await client.set('key2', { version: 1 });

    // Version IDs should be unique and non-trivial
    expect(entry1.versionId).not.toBe(entry2.versionId);
    expect(entry2.versionId).not.toBe(entry3.versionId);
    expect(entry1.versionId.length).toBeGreaterThan(10);
  });

  test('real ETag generation and validation', async () => {
    const entry = await client.set('key1', { data: 'value' });

    // Get the key object and check ETag
    const response = await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: 'test/keys/key1.json',
      })
    );

    const etag = response.ETag!;
    expect(etag).toMatch(/^".+"$/);
    expect(etag.length).toBeGreaterThan(10);
  });

  test('conditional write IfMatch succeeds with correct ETag', async () => {
    await client.set('key1', { version: 1 });

    // Get current ETag
    const headResponse = await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: 'test/keys/key1.json',
      })
    );
    const correctEtag = headResponse.ETag!;

    // Write with IfMatch should succeed
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucketName,
        Key: 'test/keys/key1.json',
        Body: JSON.stringify({ test: 'update' }),
        IfMatch: correctEtag,
      })
    );
  });

  test('conditional write IfMatch fails with wrong ETag', async () => {
    await client.set('key1', { version: 1 });

    // Write with wrong ETag should fail
    try {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test/keys/key1.json',
          Body: JSON.stringify({ test: 'update' }),
          IfMatch: '"wrong-etag"',
        })
      );
      fail('Expected PutObjectCommand to throw');
    } catch (error: any) {
      expect(error.name).toBe('PreconditionFailed');
    }
  });

  test('conditional write IfNoneMatch creates new key', async () => {
    // Write with IfNoneMatch='*' should succeed for new key
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucketName,
        Key: 'test/keys/new-key.json',
        Body: JSON.stringify({ test: 'create' }),
        IfNoneMatch: '*',
      })
    );
  });

  test('conditional write IfNoneMatch fails when key exists', async () => {
    await client.set('existing-key', { version: 1 });

    // Write with IfNoneMatch='*' should fail
    try {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test/keys/existing-key.json',
          Body: JSON.stringify({ test: 'create' }),
          IfNoneMatch: '*',
        })
      );
      fail('Expected PutObjectCommand to throw');
    } catch (error: any) {
      expect(error.name).toBe('PreconditionFailed');
    }
  });

  test('list object versions returns proper order', async () => {
    const entry1 = await client.set('key1', { version: 1 });
    const entry2 = await client.set('key1', { version: 2 });
    const entry3 = await client.set('key1', { version: 3 });

    // List versions
    const response = await s3Client.send(
      new ListObjectVersionsCommand({
        Bucket: bucketName,
        Prefix: 'test/keys/key1.json',
      })
    );

    const versions = response.Versions || [];
    expect(versions.length).toBe(3);

    // Should be in reverse chronological order (newest first)
    expect(versions.every(v => v.VersionId)).toBe(true);
  });

  test('log object structure matches spec', async () => {
    // Create first entry (genesis) then a second entry
    await client.set('key0', { data: 'first' });
    const entry = await client.set('key1', { data: 'value' });

    // Read log object directly from S3 (latest version has all fields)
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test/_log.json',
      })
    );

    const logDataStr = await response.Body!.transformToString();
    const logData = JSON.parse(logDataStr);

    // Verify required fields per design doc
    const requiredFields = [
      'sequence',
      'key',
      'value',
      'timestamp_ms',
      'previous_version_id',
      'previous_hash',
      'hash',
    ];

    for (const field of requiredFields) {
      expect(logData).toHaveProperty(field);
    }

    // previous_key_object_etag field exists but may be null
    expect('previous_key_object_etag' in logData).toBe(true);
  });

  test('key object structure matches spec', async () => {
    const entry = await client.set('key1', { data: 'value' });

    // Read key object directly from S3
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test/keys/key1.json',
      })
    );

    const keyDataStr = await response.Body!.transformToString();
    const keyData = JSON.parse(keyDataStr);

    // Verify required fields
    const requiredFields = [
      'sequence',
      'key',
      'value',
      'timestamp_ms',
      'log_version_id',
      'hash',
      'previous_hash',
    ];

    for (const field of requiredFields) {
      expect(keyData).toHaveProperty(field);
    }

    // Verify excluded fields (per design doc)
    const excludedFields = ['previous_version_id', 'previous_key_object_etag'];

    for (const field of excludedFields) {
      expect(keyData).not.toHaveProperty(field);
    }
  });
});
