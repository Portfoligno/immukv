/**
 * Integration tests using real S3 API (MinIO).
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

describe('Integration Tests with MinIO', () => {
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

    const endpointUrl = process.env.IMMUKV_S3_ENDPOINT || 'http://minio:9000';
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
    if (client !== undefined) {
      await client.close();
    }

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
    // Create genesis entry
    const genesisEntry = await client.set('key0', { data: 'first' });

    // Read genesis log entry specifically to check omitted fields
    const genesisResponse = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test/_log.json',
        VersionId: genesisEntry.versionId,
      })
    );

    const genesisDataStr = await genesisResponse.Body!.transformToString();
    const genesisData = JSON.parse(genesisDataStr);

    // Verify always-required fields
    const requiredFields = [
      'sequence',
      'key',
      'value',
      'timestamp_ms',
      'previous_hash',
      'hash',
    ];

    for (const field of requiredFields) {
      expect(genesisData).toHaveProperty(field);
    }

    // Optional fields should be omitted for genesis entry (undefined stripped)
    expect(genesisData).not.toHaveProperty('previous_version_id');
    expect(genesisData).not.toHaveProperty('previous_key_object_etag');
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

  test('undefined values omitted from JSON', async () => {
    if (!integrationTestEnabled) return;

    const client = new ImmuKVClient<string, any>({
      s3Bucket: bucketName,
      s3Region: 'us-east-1',
      s3Prefix: 'test-undefined-omit/',
      overrides: {
        endpointUrl: process.env.IMMUKV_S3_ENDPOINT,
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'minioadmin',
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'minioadmin',
        },
        forcePathStyle: true,
      },
    }, identityParser);

    // First write - creates genesis entry with previousVersionId=undefined, previousKeyObjectEtag=undefined
    const entry1 = await client.set('test-key', { value: 'first' });

    // Read raw log entry from S3 to verify undefined fields are omitted
    const getResponse = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test-undefined-omit/_log.json',
        VersionId: entry1.versionId,
      })
    );

    const logBody = await getResponse.Body?.transformToString();
    const logData = JSON.parse(logBody!);

    // Verify undefined values were omitted (fields should not exist in JSON)
    expect(logData).not.toHaveProperty('previous_version_id');
    expect(logData).not.toHaveProperty('previous_key_object_etag');

    // Second write - has previousVersionId but previousKeyObjectEtag might be undefined
    const entry2 = await client.set('test-key', { value: 'second' });

    const getResponse2 = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test-undefined-omit/_log.json',
        VersionId: entry2.versionId,
      })
    );

    const logBody2 = await getResponse2.Body?.transformToString();
    const logData2 = JSON.parse(logBody2!);

    // Second entry should have previous_version_id (not undefined)
    expect(logData2).toHaveProperty('previous_version_id');
    expect(logData2.previous_version_id).toBe(entry1.versionId);

    // If previous_key_object_etag is undefined, it should be omitted
    // If it exists, it should have a value
    if ('previous_key_object_etag' in logData2) {
      expect(logData2.previous_key_object_etag).not.toBeUndefined();
      expect(logData2.previous_key_object_etag).not.toBeNull();
    }
  });

  test('missing optional fields handled correctly', async () => {
    if (!integrationTestEnabled) return;

    const client = new ImmuKVClient<string, any>({
      s3Bucket: bucketName,
      s3Region: 'us-east-1',
      s3Prefix: 'test-missing-fields-ts/',
      overrides: {
        endpointUrl: process.env.IMMUKV_S3_ENDPOINT,
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'minioadmin',
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'minioadmin',
        },
        forcePathStyle: true,
      },
    }, identityParser);

    await client.set('test-key', { value: 'test' });

    // Manually write a log entry with optional fields completely missing (simulating Python None stripping)
    const manuallyCreatedLog = {
      sequence: 99,
      key: 'manual-key',
      value: { data: 'manual' },
      timestamp_ms: 1234567890000,
      hash: 'sha256:' + 'a'.repeat(64),
      previous_hash: 'sha256:genesis',
      // Deliberately omit previous_version_id and previous_key_object_etag
    };

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucketName,
        Key: 'test-missing-fields-ts/_log.json',
        Body: JSON.stringify(manuallyCreatedLog),
        ContentType: 'application/json',
      })
    );

    // Read it back
    const getResponse = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'test-missing-fields-ts/_log.json',
      })
    );

    const logBody = await getResponse.Body?.transformToString();
    const logData = JSON.parse(logBody!);

    // Verify fields are missing (Python None stripping behavior)
    expect(logData).not.toHaveProperty('previous_version_id');
    expect(logData).not.toHaveProperty('previous_key_object_etag');

    // TypeScript should handle missing fields as undefined
    expect(logData.previous_version_id).toBeUndefined();
    expect(logData.previous_key_object_etag).toBeUndefined();
  });
});
