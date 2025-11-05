/**
 * Tests for ImmuKV client - Simple S3 versioning implementation.
 */

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectVersionsCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { ImmuKVClient } from '../src/client';
import { Config, KeyNotFoundError } from '../src/types';
import { JSONValue, ValueParser } from '../src/jsonHelpers';
const s3Mock = mockClient(S3Client);

// Identity parser for tests (value is any, so just pass through)
const identityParser: ValueParser<any> = (value: JSONValue) => value;

// Mock Body type matching AWS SDK's SdkStreamMixin interface
interface MockBody {
  transformToString(): Promise<string>;
}

// Helper to create a mock Body object with transformToString method
function createMockBody(data: string): MockBody {
  return {
    transformToString: async () => data,
  };
}

describe('ImmuKVClient', () => {
  let client: ImmuKVClient;
  let config: Config;

  // Mock storage to simulate S3 versioning
  const mockStorage: Map<
    string,
    { versions: Array<{ versionId: string; data: string; etag: string }> }
  > = new Map();
  let versionCounter = 1;

  beforeEach(() => {
    s3Mock.reset();
    mockStorage.clear();
    versionCounter = 1;

    config = {
      s3Bucket: 'test-bucket',
      s3Region: 'us-east-1',
      s3Prefix: 'test/',
      repairCheckIntervalMs: 1000,
    };

    // Setup S3 mock responses
    setupMockS3();

    client = new ImmuKVClient(config, identityParser);
  });

  afterEach(() => {
    client.close();
  });

  function setupMockS3() {
    // Mock PutObject - simulates writing with versioning
    s3Mock.on(PutObjectCommand).callsFake(params => {
      const key = params.Key!;
      const body = params.Body as string;
      const versionId = `v${versionCounter++}`;
      const etag = `"etag-${versionId}"`;

      if (!mockStorage.has(key)) {
        mockStorage.set(key, { versions: [] });
      }

      const storage = mockStorage.get(key)!;

      // Check IfMatch/IfNoneMatch conditions
      if (params.IfMatch) {
        const latestVersion = storage.versions[storage.versions.length - 1];
        if (!latestVersion || latestVersion.etag !== params.IfMatch) {
          throw { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } };
        }
      }

      if (params.IfNoneMatch === '*') {
        if (storage.versions.length > 0) {
          throw { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } };
        }
      }

      storage.versions.push({ versionId, data: body, etag });

      return Promise.resolve({ VersionId: versionId, ETag: etag });
    });

    // Mock GetObject - retrieves specific version or latest
    s3Mock.on(GetObjectCommand).callsFake(params => {
      const key = params.Key!;
      const storage = mockStorage.get(key);

      if (!storage || storage.versions.length === 0) {
        throw { name: 'NoSuchKey', $metadata: { httpStatusCode: 404 } };
      }

      let version;
      if (params.VersionId) {
        version = storage.versions.find(v => v.versionId === params.VersionId);
        if (!version) {
          throw { name: 'NoSuchVersion', $metadata: { httpStatusCode: 404 } };
        }
      } else {
        version = storage.versions[storage.versions.length - 1];
      }

      return Promise.resolve({
        Body: createMockBody(version.data),
        VersionId: version.versionId,
        ETag: version.etag,
      });
    });

    // Mock HeadObject - gets metadata
    s3Mock.on(HeadObjectCommand).callsFake(params => {
      const key = params.Key!;
      const storage = mockStorage.get(key);

      if (!storage || storage.versions.length === 0) {
        throw { name: 'NotFound', $metadata: { httpStatusCode: 404 } };
      }

      const version = storage.versions[storage.versions.length - 1];
      return Promise.resolve({
        VersionId: version.versionId,
        ETag: version.etag,
      });
    });

    // Mock ListObjectVersions - lists all versions
    s3Mock.on(ListObjectVersionsCommand).callsFake(params => {
      const prefix = params.Prefix!;
      const storage = mockStorage.get(prefix);

      if (!storage || storage.versions.length === 0) {
        return Promise.resolve({ Versions: [] });
      }

      const versions = storage.versions.map(v => ({
        Key: prefix,
        VersionId: v.versionId,
        IsLatest: v === storage.versions[storage.versions.length - 1],
        ETag: v.etag,
      }));

      // Return in reverse order (newest first)
      return Promise.resolve({ Versions: versions.reverse(), IsTruncated: false });
    });

    // Mock ListObjectsV2 - lists keys
    s3Mock.on(ListObjectsV2Command).callsFake(params => {
      const prefix = params.Prefix || '';
      const startAfter = params.StartAfter;
      const contents: Array<{ Key: string }> = [];

      for (const [key] of mockStorage) {
        if (key.startsWith(prefix)) {
          contents.push({ Key: key });
        }
      }

      // Sort lexicographically
      contents.sort((a, b) => a.Key!.localeCompare(b.Key!));

      // Filter by StartAfter if provided
      let filteredContents = contents;
      if (startAfter) {
        filteredContents = contents.filter(item => item.Key! > startAfter);
      }

      return Promise.resolve({ Contents: filteredContents, IsTruncated: false });
    });
  }

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

      const [entries, oldestVersion] = await client.history('metric', null, null);

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

      const [entries] = await client.history('counter', null, 3);

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

      const [entries] = await client.history('key-x', null, null);

      expect(entries).toHaveLength(3);
      expect(entries.every(e => e.key === 'key-x')).toBe(true);
      expect(entries[0].value).toEqual({ data: 'x3' });
      expect(entries[1].value).toEqual({ data: 'x2' });
      expect(entries[2].value).toEqual({ data: 'x1' });
    });

    test('history for nonexistent key returns empty array', async () => {
      await client.set('other-key', { data: 'value' });

      const [entries, oldestVersion] = await client.history('nonexistent-key', null, null);

      expect(entries).toEqual([]);
      expect(oldestVersion).toBeNull();
    });
  });

  describe('Log Operations', () => {
    test('logEntries returns all entries in descending order', async () => {
      await client.set('k1', { v: 1 });
      await client.set('k2', { v: 2 });
      await client.set('k1', { v: 3 });

      const entries = await client.logEntries(null, null);

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

      const entries = await client.logEntries(null, 3);

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

      const keys = await client.listKeys(null, null);

      expect(keys).toHaveLength(3);
      expect(keys).toEqual(['apple', 'banana', 'zebra']);
    });

    test('listKeys with limit returns only requested number', async () => {
      for (let i = 0; i < 5; i++) {
        await client.set(`key-${i.toString().padStart(2, '0')}`, { index: i });
      }

      const keys = await client.listKeys(null, 3);

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

      const roClient = new ImmuKVClient(roConfig, identityParser);

      const entry = await roClient.get('readonly-test');
      expect(entry.value).toEqual({ value: 'data' });

      roClient.close();
    });
  });
});
