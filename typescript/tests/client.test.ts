/**
 * Integration tests for ImmuKV client using MinIO.
 *
 * These tests require MinIO running and test actual S3 operations.
 * Run with: IMMUKV_INTEGRATION_TEST=true IMMUKV_S3_ENDPOINT=http://localhost:9000 npm test
 */

import {
  S3Client,
  CreateBucketCommand,
  PutBucketVersioningCommand,
  ListObjectVersionsCommand,
  DeleteObjectCommand,
  DeleteBucketCommand,
} from '@aws-sdk/client-s3';
import { v4 as uuidv4 } from 'uuid';
import { ImmuKVClient } from '../src/client';
import { Config, KeyNotFoundError } from '../src/types';
import { JSONValue, ValueDecoder, ValueEncoder } from '../src/jsonHelpers';

const integrationTestEnabled = process.env.IMMUKV_INTEGRATION_TEST === 'true';

// Identity decoder and encoder for tests (value is any, so just pass through)
const identityDecoder: ValueDecoder<any> = (value: JSONValue) => value;
const identityEncoder: ValueEncoder<any> = (value: any) => value as JSONValue;

describe('ImmuKVClient', () => {
  let s3Client: S3Client;
  let bucketName: string;
  let client: ImmuKVClient;
  let config: Config;

  if (!integrationTestEnabled) {
    test.skip('Integration tests require IMMUKV_INTEGRATION_TEST=true', () => {});
    return;
  }

  beforeEach(async () => {
    // Create unique bucket per test for complete isolation
    bucketName = `test-immukv-${uuidv4().substring(0, 8)}`;

    const endpointUrl = process.env.IMMUKV_S3_ENDPOINT || 'http://localhost:4566';
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

    config = {
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

    client = new ImmuKVClient(config, identityDecoder, identityEncoder);
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

  describe('Basic Operations', () => {
    test('set and get single entry', async () => {
      const entry = await client.set('key1', { data: 'value1' });

      expect(entry.key).toBe('key1');
      expect(entry.value).toEqual({ data: 'value1' });
      expect(entry.sequence).toBe(0);
      expect(entry.previousHash).toBe('sha256:genesis');
      expect(entry.hash).toMatch(/^sha256:[a-f0-9]{64}$/);
      expect(entry.versionId).toBeDefined();
      expect(entry.previousVersionId).toBeUndefined();

      const retrieved = await client.get('key1');
      expect(retrieved.key).toBe(entry.key);
      expect(retrieved.value).toEqual(entry.value);
      expect(retrieved.sequence).toBe(entry.sequence);
      expect(retrieved.hash).toBe(entry.hash);
    });

    test('set multiple entries to same key', async () => {
      const entry1 = await client.set('sensor-01', { temp: 20.5 });
      const entry2 = await client.set('sensor-01', { temp: 21.0 });
      const entry3 = await client.set('sensor-01', { temp: 19.8 });

      expect(entry1.sequence).toBe(0);
      expect(entry2.sequence).toBe(1);
      expect(entry3.sequence).toBe(2);

      expect(entry1.previousHash).toBe('sha256:genesis');
      expect(entry2.previousHash).toBe(entry1.hash);
      expect(entry3.previousHash).toBe(entry2.hash);

      const latest = await client.get('sensor-01');
      expect(latest.value).toEqual({ temp: 19.8 });
      expect(latest.sequence).toBe(2);
    });

    test('set multiple different keys', async () => {
      const entry1 = await client.set('key-a', { value: 'a' });
      const entry2 = await client.set('key-b', { value: 'b' });
      const entry3 = await client.set('key-c', { value: 'c' });

      expect(entry1.sequence).toBe(0);
      expect(entry2.sequence).toBe(1);
      expect(entry3.sequence).toBe(2);

      const valueA = await client.get('key-a');
      const valueB = await client.get('key-b');
      const valueC = await client.get('key-c');

      expect(valueA.value).toEqual({ value: 'a' });
      expect(valueB.value).toEqual({ value: 'b' });
      expect(valueC.value).toEqual({ value: 'c' });
    });

    test('get nonexistent key throws KeyNotFoundError', async () => {
      await client.set('existing-key', { data: 'value' });

      await expect(client.get('nonexistent-key')).rejects.toThrow(KeyNotFoundError);
    });
  });

  describe('History Operations', () => {
    test('history returns all entries for a key in descending order', async () => {
      await client.set('metric', { count: 1 });
      await client.set('metric', { count: 2 });
      await client.set('metric', { count: 3 });

      const [entries, oldestVersion] = await client.history('metric', undefined, undefined);

      expect(entries).toHaveLength(3);
      expect(entries[0].value).toEqual({ count: 3 });
      expect(entries[1].value).toEqual({ count: 2 });
      expect(entries[2].value).toEqual({ count: 1 });

      expect(entries[0].sequence).toBe(2);
      expect(entries[1].sequence).toBe(1);
      expect(entries[2].sequence).toBe(0);
    });

    test('history with limit returns only requested number of entries', async () => {
      for (let i = 0; i < 5; i++) {
        await client.set('counter', { value: i });
      }

      const [entries] = await client.history('counter', undefined, 3);

      expect(entries).toHaveLength(3);
      expect(entries[0].value).toEqual({ value: 4 });
      expect(entries[1].value).toEqual({ value: 3 });
      expect(entries[2].value).toEqual({ value: 2 });
    });

    test('history returns only entries for requested key', async () => {
      await client.set('key-x', { data: 'x1' });
      await client.set('key-y', { data: 'y1' });
      await client.set('key-x', { data: 'x2' });
      await client.set('key-y', { data: 'y2' });
      await client.set('key-x', { data: 'x3' });

      const [entries] = await client.history('key-x', undefined, undefined);

      expect(entries).toHaveLength(3);
      expect(entries.every(e => e.key === 'key-x')).toBe(true);
      expect(entries[0].value).toEqual({ data: 'x3' });
      expect(entries[1].value).toEqual({ data: 'x2' });
      expect(entries[2].value).toEqual({ data: 'x1' });
    });

    test('history for nonexistent key returns empty array', async () => {
      await client.set('other-key', { data: 'value' });

      const [entries, oldestVersion] = await client.history(
        'nonexistent-key',
        undefined,
        undefined
      );

      expect(entries).toEqual([]);
      expect(oldestVersion).toBeUndefined();
    });
  });

  describe('Log Operations', () => {
    test('logEntries returns all entries in descending order', async () => {
      await client.set('k1', { v: 1 });
      await client.set('k2', { v: 2 });
      await client.set('k1', { v: 3 });

      const entries = await client.logEntries(undefined, undefined);

      expect(entries).toHaveLength(3);
      expect(entries[0].key).toBe('k1');
      expect(entries[0].value).toEqual({ v: 3 });
      expect(entries[0].sequence).toBe(2);

      expect(entries[1].key).toBe('k2');
      expect(entries[1].value).toEqual({ v: 2 });
      expect(entries[1].sequence).toBe(1);

      expect(entries[2].key).toBe('k1');
      expect(entries[2].value).toEqual({ v: 1 });
      expect(entries[2].sequence).toBe(0);
    });

    test('logEntries with limit returns only requested number', async () => {
      for (let i = 0; i < 5; i++) {
        await client.set(`key-${i}`, { index: i });
      }

      const entries = await client.logEntries(undefined, 3);

      expect(entries).toHaveLength(3);
      expect(entries[0].sequence).toBe(4);
      expect(entries[1].sequence).toBe(3);
      expect(entries[2].sequence).toBe(2);
    });
  });

  describe('Key Listing', () => {
    test('listKeys returns all keys in lexicographic order', async () => {
      await client.set('zebra', { animal: 'z' });
      await client.set('apple', { fruit: 'a' });
      await client.set('banana', { fruit: 'b' });

      const keys = await client.listKeys(undefined, undefined);

      expect(keys).toHaveLength(3);
      expect(keys).toEqual(['apple', 'banana', 'zebra']);
    });

    test('listKeys with limit returns only requested number', async () => {
      for (let i = 0; i < 5; i++) {
        await client.set(`key-${i.toString().padStart(2, '0')}`, { index: i });
      }

      const keys = await client.listKeys(undefined, 3);

      expect(keys).toHaveLength(3);
      expect(keys).toEqual(['key-00', 'key-01', 'key-02']);
    });

    test('listKeys with afterKey returns keys after specified key', async () => {
      for (let i = 0; i < 5; i++) {
        await client.set(`key-${i.toString().padStart(2, '0')}`, { index: i });
      }

      const keys = await client.listKeys('key-01', 2);

      expect(keys).toHaveLength(2);
      expect(keys).toEqual(['key-02', 'key-03']);
    });
  });

  describe('Verification', () => {
    test('verify returns true for valid entry', async () => {
      const entry = await client.set('test-key', { field: 'value' });

      expect(await client.verify(entry)).toBe(true);
    });

    test('verify returns false for corrupted entry', async () => {
      const entry = await client.set('test-key', { field: 'value' });

      // Corrupt the entry
      entry.value = { field: 'corrupted' };

      expect(await client.verify(entry)).toBe(false);
    });

    test('verifyLogChain returns true for valid chain', async () => {
      await client.set('k1', { v: 1 });
      await client.set('k2', { v: 2 });
      await client.set('k3', { v: 3 });

      const result = await client.verifyLogChain();

      expect(result).toBe(true);
    });

    test('verifyLogChain with limit verifies only recent entries', async () => {
      for (let i = 0; i < 10; i++) {
        await client.set(`key-${i}`, { index: i });
      }

      const result = await client.verifyLogChain(5);

      expect(result).toBe(true);
    });
  });

  describe('Hash Chain Integrity', () => {
    test('entries form correct hash chain', async () => {
      const entry1 = await client.set('chain-test', { step: 1 });
      const entry2 = await client.set('chain-test', { step: 2 });
      const entry3 = await client.set('chain-test', { step: 3 });

      expect(entry1.previousHash).toBe('sha256:genesis');
      expect(entry2.previousHash).toBe(entry1.hash);
      expect(entry3.previousHash).toBe(entry2.hash);

      expect(entry1.hash).not.toBe(entry2.hash);
      expect(entry2.hash).not.toBe(entry3.hash);
      expect(entry1.hash).not.toBe(entry3.hash);
    });

    test('sequence numbers are contiguous across different keys', async () => {
      const entries = [];

      entries.push(await client.set('a', { v: 1 }));
      entries.push(await client.set('b', { v: 2 }));
      entries.push(await client.set('a', { v: 3 }));
      entries.push(await client.set('c', { v: 4 }));
      entries.push(await client.set('b', { v: 5 }));

      for (let i = 0; i < entries.length; i++) {
        expect(entries[i].sequence).toBe(i);
      }
    });
  });

  describe('Version Retrieval', () => {
    test('getLogVersion retrieves specific log entry by version id', async () => {
      const entry1 = await client.set('versioned', { data: 'first' });
      const entry2 = await client.set('versioned', { data: 'second' });

      const retrieved = await client.getLogVersion(entry1.versionId);

      expect(retrieved.sequence).toBe(entry1.sequence);
      expect(retrieved.value).toEqual({ data: 'first' });
      expect(retrieved.hash).toBe(entry1.hash);
    });
  });

  describe('Read-Only Mode', () => {
    test('read-only client can read existing data', async () => {
      await client.set('readonly-test', { value: 'data' });

      const roConfig: Config = {
        ...config,
        readOnly: true,
      };

      const roClient = new ImmuKVClient(roConfig, identityDecoder, identityEncoder);

      const entry = await roClient.get('readonly-test');
      expect(entry.value).toEqual({ value: 'data' });

      roClient.close();
    });
  });

  describe('Custom Endpoint URL', () => {
    test('accepts overrides for S3-compatible services', () => {
      const customConfig: Config = {
        s3Bucket: 'test-bucket',
        s3Region: 'us-east-1',
        s3Prefix: 'test/',
        overrides: {
          endpointUrl: 'http://localhost:4566',
        },
      };

      // Should not throw when creating client with overrides
      const customClient = new ImmuKVClient(customConfig, identityDecoder, identityEncoder);
      customClient.close();
    });

    test('works without overrides for AWS S3', () => {
      const defaultConfig: Config = {
        s3Bucket: 'test-bucket',
        s3Region: 'us-east-1',
        s3Prefix: 'test/',
      };

      // Should not throw when creating client without overrides
      const defaultClient = new ImmuKVClient(defaultConfig, identityDecoder, identityEncoder);
      defaultClient.close();
    });
  });

  describe('Orphan status boolean checks', () => {
    test('is_orphaned=false does not trigger orphan fallback in get()', async () => {
      // Create an entry and set it as orphan status with isOrphaned=false
      const entry = await client.set('test-key', { value: 'orphan_data' });

      // Set read-only mode so orphan fallback would be checked
      client['canWrite'] = false;

      client['latestOrphanStatus'] = {
        isOrphaned: false, // Explicitly false - repair completed
        orphanKey: 'nonexistent-key',
        orphanEntry: entry,
        checkedAt: 0,
      };

      // get() should throw KeyNotFoundError, NOT return the orphan entry
      // Even though all other conditions match (read-only, key matches, entry exists)
      // isOrphaned=false should prevent the orphan fallback
      await expect(client.get('nonexistent-key')).rejects.toThrow(
        "Key 'nonexistent-key' not found"
      );
    });

    test('is_orphaned=true returns orphan entry in get()', async () => {
      // Create an entry
      const entry = await client.set('existing-key', { value: 'test_data' });

      // Set read-only mode
      client['canWrite'] = false;

      // Simulate orphan status with isOrphaned=true for a nonexistent key
      client['latestOrphanStatus'] = {
        isOrphaned: true, // Explicitly true - orphan exists
        orphanKey: 'orphaned-key',
        orphanEntry: entry,
        checkedAt: 0,
      };

      // get() on the orphaned key should return the orphan entry (not throw)
      // Even though the key object doesn't exist in S3
      const result = await client.get('orphaned-key');

      // Should return the cached orphan entry
      expect(result.value).toEqual({ value: 'test_data' });
      expect(result.key).toBe('existing-key'); // Original key from entry
    });

    test('is_orphaned=false does not prepend orphan entry in history()', async () => {
      // Create a key with history
      await client.set('test-key', { value: 'v1' });
      const entry2 = await client.set('test-key', { value: 'v2' });

      // Set orphan status with isOrphaned=false
      client['latestOrphanStatus'] = {
        isOrphaned: false, // Explicitly false
        orphanKey: 'test-key',
        orphanEntry: entry2,
        checkedAt: 0,
      };

      // Get history - should NOT include orphan entry as first item
      const [entries] = await client.history('test-key', undefined, undefined);

      // Should have 2 entries (v2 and v1), NOT 3 (orphan + v2 + v1)
      expect(entries).toHaveLength(2);
      expect(entries[0].value).toEqual({ value: 'v2' });
      expect(entries[1].value).toEqual({ value: 'v1' });
    });

    test('is_orphaned=true prepends orphan entry in history()', async () => {
      // Create a key with history
      await client.set('test-key', { value: 'v1' });
      const entry2 = await client.set('test-key', { value: 'v2' });

      // Set orphan status with isOrphaned=true
      client['latestOrphanStatus'] = {
        isOrphaned: true, // Explicitly true
        orphanKey: 'test-key',
        orphanEntry: entry2,
        checkedAt: 0,
      };

      // Get history - should include orphan entry as first item
      const [entries] = await client.history('test-key', undefined, undefined);

      // Should have 3 entries: orphan (v2) + v2 + v1
      // Note: This creates a duplicate entry, which is the expected behavior
      // when orphan repair hasn't completed yet
      expect(entries).toHaveLength(3);
      expect(entries[0].value).toEqual({ value: 'v2' }); // Orphan entry
      expect(entries[1].value).toEqual({ value: 'v2' }); // Actual latest
      expect(entries[2].value).toEqual({ value: 'v1' });
    });

    test('is_orphaned=undefined does not trigger orphan fallback', async () => {
      // Create an entry
      const entry = await client.set('test-key', { value: 'test_data' });

      // Set orphan status with isOrphaned=undefined (missing field)
      client['latestOrphanStatus'] = {
        isOrphaned: undefined, // Explicitly undefined
        orphanKey: 'nonexistent-key',
        orphanEntry: entry,
      } as any;

      // get() on nonexistent key should throw KeyNotFoundError
      await expect(client.get('nonexistent-key')).rejects.toThrow(
        "Key 'nonexistent-key' not found"
      );
    });
  });
});
