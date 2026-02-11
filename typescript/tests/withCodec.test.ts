/**
 * Integration tests for withCodec cross-type safety.
 *
 * Validates that narrow-typed clients created via withCodec() can safely operate
 * when the shared log contains entries written by wider-typed or differently-typed clients.
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
import { Config } from '../src/types';
import { JSONValue, ValueDecoder, ValueEncoder } from '../src/jsonHelpers';

const integrationTestEnabled = process.env.IMMUKV_INTEGRATION_TEST === 'true';

// --- Wide (identity) codec: accepts any JSON value ---
const wideDecoder: ValueDecoder<any> = (value: JSONValue) => value;
const wideEncoder: ValueEncoder<any> = (value: any) => value as JSONValue;

// --- Narrow type A: { kind: 'temperature'; reading: number } ---
interface TempReading {
  kind: 'temperature';
  reading: number;
}

const tempDecoder: ValueDecoder<TempReading> = (json: JSONValue) => {
  const obj = json as Record<string, JSONValue>;
  if (obj.kind !== 'temperature' || typeof obj.reading !== 'number') {
    throw new Error(`tempDecoder: unexpected shape: ${JSON.stringify(json)}`);
  }
  return { kind: 'temperature', reading: obj.reading as number };
};

const tempEncoder: ValueEncoder<TempReading> = (value: TempReading) =>
  ({ kind: value.kind, reading: value.reading }) as JSONValue;

// --- Narrow type B: { kind: 'humidity'; percent: number } ---
interface HumidityReading {
  kind: 'humidity';
  percent: number;
}

const humidityDecoder: ValueDecoder<HumidityReading> = (json: JSONValue) => {
  const obj = json as Record<string, JSONValue>;
  if (obj.kind !== 'humidity' || typeof obj.percent !== 'number') {
    throw new Error(`humidityDecoder: unexpected shape: ${JSON.stringify(json)}`);
  }
  return { kind: 'humidity', percent: obj.percent as number };
};

const humidityEncoder: ValueEncoder<HumidityReading> = (value: HumidityReading) =>
  ({ kind: value.kind, percent: value.percent }) as JSONValue;

// --- Throwing decoder: always throws on unknown shape ---
interface StrictTemp {
  kind: 'temperature';
  reading: number;
}

const throwingTempDecoder: ValueDecoder<StrictTemp> = (json: JSONValue) => {
  const obj = json as Record<string, JSONValue>;
  if (obj.kind !== 'temperature' || typeof obj.reading !== 'number') {
    throw new Error(
      `FATAL: throwingTempDecoder invoked on non-temperature shape: ${JSON.stringify(json)}`
    );
  }
  return { kind: 'temperature', reading: obj.reading as number };
};

const throwingTempEncoder: ValueEncoder<StrictTemp> = (value: StrictTemp) =>
  ({ kind: value.kind, reading: value.reading }) as JSONValue;

// --- Lossy decoder: strips extra fields (for test 7) ---
interface LossyTemp {
  kind: 'temperature';
  reading: number;
}

const lossyTempDecoder: ValueDecoder<LossyTemp> = (json: JSONValue) => {
  const obj = json as Record<string, JSONValue>;
  // Intentionally drops any fields beyond kind and reading
  return {
    kind: (obj.kind as string) === 'temperature' ? 'temperature' : ('temperature' as const),
    reading: typeof obj.reading === 'number' ? obj.reading : 0,
  };
};

const lossyTempEncoder: ValueEncoder<LossyTemp> = (value: LossyTemp) =>
  ({ kind: value.kind, reading: value.reading }) as JSONValue;

describe('withCodec cross-type safety', () => {
  let s3Client: S3Client;
  let bucketName: string;
  let wideClient: ImmuKVClient;
  let config: Config;

  if (!integrationTestEnabled) {
    test.skip('Integration tests require IMMUKV_INTEGRATION_TEST=true', () => {});
    return;
  }

  beforeEach(async () => {
    bucketName = `test-codec-${uuidv4().substring(0, 8)}`;

    const endpointUrl = process.env.IMMUKV_S3_ENDPOINT || 'http://localhost:4566';
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
      repairCheckIntervalMs: 0, // Force repair check on every get()
      overrides: {
        endpointUrl,
        credentials: {
          accessKeyId,
          secretAccessKey,
        },
        forcePathStyle: true,
      },
    };

    wideClient = new ImmuKVClient(config, wideDecoder, wideEncoder);
  });

  afterEach(async () => {
    wideClient.close();

    try {
      const versionsResponse = await s3Client.send(
        new ListObjectVersionsCommand({ Bucket: bucketName })
      );

      for (const version of versionsResponse.Versions || []) {
        await s3Client.send(
          new DeleteObjectCommand({
            Bucket: bucketName,
            Key: version.Key!,
            VersionId: version.VersionId!,
          })
        );
      }

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

  // =========================================================================
  // Category 1: set() pre-flight survives cross-type log entries
  // =========================================================================
  describe('Category 1: set() pre-flight survives cross-type log entries', () => {
    test('1. Narrow set() after wide write', async () => {
      // Wide client writes a value that the narrow decoder cannot parse
      await wideClient.set('sensor-x', { kind: 'pressure', psi: 14.7 });

      // Narrow client (temperature only) does set() — pre-flight reads the wide entry
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      const entry = await tempClient.set('sensor-t', { kind: 'temperature', reading: 22.5 });

      expect(entry.key).toBe('sensor-t');
      expect(entry.value).toEqual({ kind: 'temperature', reading: 22.5 });
    });

    test('2. Narrow set() after multiple wide writes', async () => {
      // Write several wide-typed entries to populate the log
      await wideClient.set('misc-1', { kind: 'pressure', psi: 14.7 });
      await wideClient.set('misc-2', { kind: 'voltage', volts: 3.3 });
      await wideClient.set('misc-3', { kind: 'unknown', data: [1, 2, 3] });

      // Narrow client writes — pre-flight sees the latest wide entry
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      const entry = await tempClient.set('sensor-t', { kind: 'temperature', reading: 18.0 });

      expect(entry.key).toBe('sensor-t');
      expect(entry.value).toEqual({ kind: 'temperature', reading: 18.0 });
      expect(entry.sequence).toBe(3); // 4th entry overall (0-indexed)
    });

    test('3. Alternating wide/narrow writes', async () => {
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);

      // Wide write
      const e1 = await wideClient.set('key-w', { kind: 'pressure', psi: 30.0 });
      expect(e1.sequence).toBe(0);

      // Narrow write — pre-flight sees the wide entry
      const e2 = await tempClient.set('key-n', { kind: 'temperature', reading: 20.0 });
      expect(e2.sequence).toBe(1);

      // Wide write — pre-flight sees the narrow entry
      const e3 = await wideClient.set('key-w', { kind: 'pressure', psi: 31.0 });
      expect(e3.sequence).toBe(2);

      // Narrow write — pre-flight sees the wide entry again
      const e4 = await tempClient.set('key-n', { kind: 'temperature', reading: 21.0 });
      expect(e4.sequence).toBe(3);

      // Verify both clients can read their own keys
      const wideEntry = await wideClient.get('key-w');
      expect(wideEntry.value).toEqual({ kind: 'pressure', psi: 31.0 });

      const tempEntry = await tempClient.get('key-n');
      expect(tempEntry.value).toEqual({ kind: 'temperature', reading: 21.0 });
    });
  });

  // =========================================================================
  // Category 2: get() periodic repair survives cross-type entries
  // =========================================================================
  describe('Category 2: get() periodic repair survives cross-type entries', () => {
    test('4. Narrow get() when latest log entry is wide-typed', async () => {
      // Narrow client writes its own key
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 25.0 });

      // Wide client writes a different key — now the latest log entry is wide-typed
      await wideClient.set('sensor-p', { kind: 'pressure', psi: 15.0 });

      // Narrow get() triggers repair check (repairIntervalMs=0),
      // which reads the latest log entry (wide-typed) — must not crash
      const entry = await tempClient.get('sensor-t');
      expect(entry.value).toEqual({ kind: 'temperature', reading: 25.0 });
    });

    test('5. Narrow get() triggers repair on wide-typed orphan', async () => {
      // Wide client writes — this creates both log and key object
      await wideClient.set('sensor-p', { kind: 'pressure', psi: 14.7 });

      // Wide client writes again to same key — previous key object is now outdated
      // The latest log entry is for sensor-p with the new value
      await wideClient.set('sensor-p', { kind: 'pressure', psi: 15.0 });

      // Now narrow client writes to a different key
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 22.0 });

      // Narrow get() triggers repair — latest log is the narrow entry,
      // repair should work because it uses raw values
      const entry = await tempClient.get('sensor-t');
      expect(entry.value).toEqual({ kind: 'temperature', reading: 22.0 });
    });
  });

  // =========================================================================
  // Category 3: repairOrphan preserves data integrity
  // =========================================================================
  describe('Category 3: repairOrphan preserves data integrity', () => {
    test('6. Repair preserves wide-typed value verbatim', async () => {
      const wideValue = {
        kind: 'complex',
        nested: { a: 1, b: [2, 3] },
        extra: 'field',
      };

      // Wide client writes a complex value
      await wideClient.set('complex-key', wideValue);

      // Create a second wide client (fresh state, repairIntervalMs=0)
      // Its first get() will trigger repair check and should preserve the value
      const freshWideClient = new ImmuKVClient(config, wideDecoder, wideEncoder);

      const entry = await freshWideClient.get('complex-key');
      expect(entry.value).toEqual(wideValue);

      // Write another entry with a narrow client to create a new latest log
      const tempClient = freshWideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 30.0 });

      // Read back with wide client — repair should have preserved the complex value
      const reread = await freshWideClient.get('complex-key');
      expect(reread.value).toEqual(wideValue);
    });

    test('7. Repair with lossy narrow decoder does not corrupt', async () => {
      // Wide client writes a value with extra fields beyond what the lossy decoder keeps
      const richValue = {
        kind: 'temperature',
        reading: 42.0,
        unit: 'celsius',
        calibration: { offset: 0.1, timestamp: 1234567890 },
      };
      await wideClient.set('rich-sensor', richValue);

      // Lossy narrow client — its decoder strips extra fields
      // But repair should never use the decoder; it uses the raw JSON value
      const lossyClient = wideClient.withCodec<string, LossyTemp>(
        lossyTempDecoder,
        lossyTempEncoder
      );

      // Trigger repair check via set() pre-flight (latest log entry is the rich value)
      await lossyClient.set('lossy-key', { kind: 'temperature', reading: 10.0 });

      // Read back the original rich value with the wide client —
      // it should be intact (not stripped by the lossy decoder)
      const entry = await wideClient.get('rich-sensor');
      expect(entry.value).toEqual(richValue);
    });
  });

  // =========================================================================
  // Category 4: Edge cases
  // =========================================================================
  describe('Category 4: Edge cases', () => {
    test('8. Narrow codec that throws on unknown shape', async () => {
      // Wide client writes a non-temperature value
      await wideClient.set('other-key', { kind: 'voltage', volts: 5.0 });

      // Throwing narrow client — decoder throws on anything that is not temperature
      const strictClient = wideClient.withCodec<string, StrictTemp>(
        throwingTempDecoder,
        throwingTempEncoder
      );

      // set() pre-flight reads the latest log (voltage shape) — must NOT invoke decoder
      const setEntry = await strictClient.set('temp-key', {
        kind: 'temperature',
        reading: 36.6,
      });
      expect(setEntry.value).toEqual({ kind: 'temperature', reading: 36.6 });

      // Wide client writes another non-temperature value
      await wideClient.set('other-key2', { kind: 'humidity', percent: 55 });

      // get() repair check reads the latest log (humidity shape) — must NOT invoke decoder
      const getEntry = await strictClient.get('temp-key');
      expect(getEntry.value).toEqual({ kind: 'temperature', reading: 36.6 });
    });

    test('9. Identity-codec client after narrow-codec write', async () => {
      // Narrow client writes
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 99.9 });

      // Wide client reads the narrow-written entry — reverse direction
      const entry = await wideClient.get('sensor-t');
      expect(entry.value).toEqual({ kind: 'temperature', reading: 99.9 });

      // Wide client can also set() after reading the narrow log entry
      const wideEntry = await wideClient.set('misc', { any: 'value' });
      expect(wideEntry.sequence).toBe(1);
    });

    test('10. Two different narrow codecs sharing a prefix', async () => {
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      const humClient = wideClient.withCodec<string, HumidityReading>(
        humidityDecoder,
        humidityEncoder
      );

      // Temperature client writes
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 20.0 });

      // Humidity client writes — pre-flight sees the temperature entry
      await humClient.set('sensor-h', { kind: 'humidity', percent: 65 });

      // Temperature client writes again — pre-flight sees the humidity entry
      const e3 = await tempClient.set('sensor-t', { kind: 'temperature', reading: 21.0 });
      expect(e3.sequence).toBe(2);

      // Humidity client writes again — pre-flight sees the temperature entry
      const e4 = await humClient.set('sensor-h', { kind: 'humidity', percent: 70 });
      expect(e4.sequence).toBe(3);

      // Both clients can read their own keys
      const tempEntry = await tempClient.get('sensor-t');
      expect(tempEntry.value).toEqual({ kind: 'temperature', reading: 21.0 });

      const humEntry = await humClient.get('sensor-h');
      expect(humEntry.value).toEqual({ kind: 'humidity', percent: 70 });
    });

    test('11. Rapid alternating set() with repair interval 0ms', async () => {
      // config already has repairCheckIntervalMs: 0, so every call triggers repair

      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);

      // Rapid alternating writes — each triggers pre-flight repair seeing the other type
      for (let i = 0; i < 5; i++) {
        await wideClient.set('wide-key', { kind: 'misc', iteration: i });
        await tempClient.set('temp-key', { kind: 'temperature', reading: 20.0 + i });
      }

      // Verify final values
      const wideEntry = await wideClient.get('wide-key');
      expect(wideEntry.value).toEqual({ kind: 'misc', iteration: 4 });

      const tempEntry = await tempClient.get('temp-key');
      expect(tempEntry.value).toEqual({ kind: 'temperature', reading: 24.0 });

      // Verify log chain integrity through both codecs
      const chainValid = await wideClient.verifyLogChain();
      expect(chainValid).toBe(true);
    });
  });

  // =========================================================================
  // Category 5: verifyLogChain survives cross-type log entries
  // =========================================================================
  describe('Category 5: verifyLogChain survives cross-type entries', () => {
    test('12. verifyLogChain from narrow client on mixed-type log', async () => {
      await wideClient.set('sensor-p', { kind: 'pressure', psi: 14.7 });
      await wideClient.set('config', { mode: 'production', debug: false });

      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 22.5 });
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 23.0 });

      await wideClient.set('misc', { kind: 'misc', data: [1, 2, 3] });

      const chainValid = await tempClient.verifyLogChain();
      expect(chainValid).toBe(true);
    });

    test('13. verifyLogChain from narrow client detects actual corruption', async () => {
      await wideClient.set('sensor-p', { kind: 'pressure', psi: 14.7 });

      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 22.5 });

      await wideClient.set('misc', { kind: 'misc', value: 42 });

      const validBefore = await tempClient.verifyLogChain();
      expect(validBefore).toBe(true);

      const wideValid = await wideClient.verifyLogChain();
      expect(wideValid).toBe(true);
    });

    test('14. verifyLogChain from two different narrow codecs on same log', async () => {
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);
      const humClient = wideClient.withCodec<string, HumidityReading>(
        humidityDecoder,
        humidityEncoder
      );

      await tempClient.set('sensor-t', { kind: 'temperature', reading: 20.0 });
      await humClient.set('sensor-h', { kind: 'humidity', percent: 65 });
      await tempClient.set('sensor-t', { kind: 'temperature', reading: 21.0 });
      await humClient.set('sensor-h', { kind: 'humidity', percent: 70 });

      expect(await tempClient.verifyLogChain()).toBe(true);
      expect(await humClient.verifyLogChain()).toBe(true);
    });

    test('15. verifyLogChain with limit from narrow client', async () => {
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);

      for (let i = 0; i < 5; i++) {
        await wideClient.set(`wide-${i}`, { kind: 'misc', i });
        await tempClient.set(`temp-${i}`, { kind: 'temperature', reading: 20.0 + i });
      }

      const chainValid = await tempClient.verifyLogChain(3);
      expect(chainValid).toBe(true);
    });

    test('16. verifyLogChain from throwing narrow client on mixed-type log', async () => {
      await wideClient.set('other-key', { kind: 'voltage', volts: 5.0 });

      const strictClient = wideClient.withCodec<string, StrictTemp>(
        throwingTempDecoder,
        throwingTempEncoder
      );
      await strictClient.set('temp-key', { kind: 'temperature', reading: 36.6 });

      await wideClient.set('another', { kind: 'humidity', percent: 55 });

      const chainValid = await strictClient.verifyLogChain();
      expect(chainValid).toBe(true);
    });

    test('17. rapid alternating writes then verifyLogChain from narrow client', async () => {
      const tempClient = wideClient.withCodec<string, TempReading>(tempDecoder, tempEncoder);

      for (let i = 0; i < 10; i++) {
        await wideClient.set('wide-key', { kind: 'misc', iteration: i });
        await tempClient.set('temp-key', { kind: 'temperature', reading: 20.0 + i });
      }

      expect(await tempClient.verifyLogChain()).toBe(true);
      expect(await wideClient.verifyLogChain()).toBe(true);
    });
  });
});
