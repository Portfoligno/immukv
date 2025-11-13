/**
 * Pure unit tests that don't require S3 or LocalStack.
 *
 * These tests verify pure logic: hash computation, data validation,
 * type checking, and other functionality that doesn't need S3.
 */

import {
  Config,
  Hash,
  LogEntryForHash,
  TimestampMs,
  Sequence,
  hashCompute,
  hashGenesis,
  hashFromJson,
  timestampFromJson,
  sequenceFromJson,
} from '../src/types';

// --- Hash Computation Tests ---

test('hash compute returns sha256 prefix with 64 hex characters', () => {
  const data: LogEntryForHash<string, any> = {
    sequence: sequenceFromJson(0),
    key: 'test-key',
    value: { field: 'value' },
    timestampMs: timestampFromJson(1234567890000),
    previousHash: hashFromJson('sha256:genesis'),
  };

  const result = hashCompute(data);

  // Must start with 'sha256:'
  expect(result).toMatch(/^sha256:/);

  // Must be exactly 71 characters total (sha256: + 64 hex)
  expect(result.length).toBe(71);

  // Hex portion must be exactly 64 characters
  const hexPart = result.substring(7); // After 'sha256:'
  expect(hexPart.length).toBe(64);
  expect(hexPart).toMatch(/^[0-9a-f]{64}$/);
});

test('hash compute is deterministic', () => {
  const data: LogEntryForHash<string, any> = {
    sequence: sequenceFromJson(5),
    key: 'key1',
    value: { a: 1, b: 2 },
    timestampMs: timestampFromJson(1000000000000),
    previousHash: hashFromJson('sha256:' + 'abcd' + '0'.repeat(60)),
  };

  const hash1 = hashCompute(data);
  const hash2 = hashCompute(data);

  expect(hash1).toBe(hash2);
});

test('hash changes with different data', () => {
  const baseData: LogEntryForHash<string, any> = {
    sequence: sequenceFromJson(0),
    key: 'key',
    value: { x: 1 },
    timestampMs: timestampFromJson(1000000000000),
    previousHash: hashFromJson('sha256:genesis'),
  };

  const baseHash = hashCompute(baseData);

  // Change sequence
  expect(hashCompute({ ...baseData, sequence: sequenceFromJson(1) })).not.toBe(baseHash);

  // Change key
  expect(hashCompute({ ...baseData, key: 'different' })).not.toBe(baseHash);

  // Change value
  expect(hashCompute({ ...baseData, value: { x: 2 } })).not.toBe(baseHash);

  // Change timestamp
  expect(hashCompute({ ...baseData, timestampMs: timestampFromJson(2000000000000) })).not.toBe(
    baseHash
  );

  // Change previous hash
  expect(
    hashCompute({ ...baseData, previousHash: hashFromJson('sha256:' + '1'.repeat(64)) })
  ).not.toBe(baseHash);
});

test('hash genesis returns correct value', () => {
  const genesis = hashGenesis();

  expect(genesis).toBe('sha256:genesis');
});

test('hash from json accepts valid hash', () => {
  const validHash = 'sha256:' + 'a'.repeat(64);
  const result = hashFromJson(validHash);

  expect(result).toBe(validHash);
});

test('hash from json accepts genesis', () => {
  const result = hashFromJson('sha256:genesis');

  expect(result).toBe('sha256:genesis');
});

test('hash from json rejects invalid prefix', () => {
  expect(() => {
    hashFromJson('md5:' + 'a'.repeat(64));
  }).toThrow(/must start with 'sha256:'/);
});

// Note: hashFromJson only validates prefix, not hex length/format
// Actual hash validation happens during hash computation and comparison

// --- Timestamp Validation Tests ---

test('timestamp from json accepts valid epoch milliseconds', () => {
  // Year 2024
  const ts = timestampFromJson(1700000000000);

  expect(ts).toBe(1700000000000);
});

test('timestamp from json accepts large values', () => {
  // Typical: 1000000000000+ (year 2001+)
  const ts = timestampFromJson(1700000000000);
  expect(ts).toBe(1700000000000);
});

test('timestamp from json rejects zero', () => {
  expect(() => {
    timestampFromJson(0);
  }).toThrow(/must be > 0/);
});

test('timestamp from json rejects negative', () => {
  expect(() => {
    timestampFromJson(-1);
  }).toThrow(/must be > 0/);
});

// --- Config Validation Tests ---

test('config requires required fields', () => {
  const config: Config = {
    s3Bucket: 'test-bucket',
    s3Region: 'us-east-1',
    s3Prefix: 'test/',
  };

  expect(config.s3Bucket).toBe('test-bucket');
  expect(config.s3Region).toBe('us-east-1');
  expect(config.s3Prefix).toBe('test/');
});

test('config optional fields have correct defaults', () => {
  const config: Config = {
    s3Bucket: 'test-bucket',
    s3Region: 'us-east-1',
    s3Prefix: 'test/',
  };

  expect(config.kmsKeyId).toBeUndefined();
  expect(config.overrides).toBeUndefined();
  expect(config.repairCheckIntervalMs).toBeUndefined(); // Will default to 300000 in client
  expect(config.readOnly).toBeUndefined(); // Will default to false in client
});

test('config accepts all optional fields', () => {
  const config: Config = {
    s3Bucket: 'test-bucket',
    s3Region: 'us-east-1',
    s3Prefix: 'test/',
    kmsKeyId: 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012',
    repairCheckIntervalMs: 60000,
    readOnly: true,
    overrides: {
      endpointUrl: 'http://localhost:4566',
    },
  };

  expect(config.kmsKeyId).toBeDefined();
  expect(config.overrides?.endpointUrl).toBe('http://localhost:4566');
  expect(config.repairCheckIntervalMs).toBe(60000);
  expect(config.readOnly).toBe(true);
});
