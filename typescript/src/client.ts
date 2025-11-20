/**
 * ImmuKV client implementation.
 */

import * as s3 from '@aws-sdk/client-s3';
import type {
  Config,
  Entry,
  Hash,
  KeyObjectETag,
  KeyVersionId,
  LogVersionId,
  Sequence,
} from './types';
import { KeyNotFoundError, ReadOnlyError } from './types';
import type { JSONValue, ValueDecoder, ValueEncoder } from './jsonHelpers';
import { BrandedS3Client } from './internal/s3Client';
import { readBodyAsJson } from './internal/s3Helpers';
import { stringifyCanonical } from './internal/jsonHelpers';
import type { LogEntryForHash, OrphanStatus } from './internal/types';
import {
  hashCompute,
  hashFromJson,
  hashGenesis,
  sequenceFromJson,
  sequenceInitial,
  sequenceNext,
  timestampFromJson,
  timestampNow,
} from './internal/types';
import {
  HeadObjectCommandOutputs,
  ObjectVersions,
  PutObjectCommandOutputs,
  S3KeyPath,
  S3KeyPaths,
  LogKey,
} from './internal/s3Types';

/**
 * Main client interface - Simple S3 versioning with auto-repair.
 */
export class ImmuKVClient<K extends string = string, V = any> {
  private config: Config;
  private s3: BrandedS3Client;
  private logKey: S3KeyPath<LogKey>;
  private valueDecoder: ValueDecoder<V>;
  private valueEncoder: ValueEncoder<V>;
  private lastRepairCheckMs: number = 0;
  private canWrite?: boolean;
  private latestOrphanStatus?: OrphanStatus<K, V>;

  constructor(config: Config, valueDecoder: ValueDecoder<V>, valueEncoder: ValueEncoder<V>) {
    this.config = {
      repairCheckIntervalMs: 300000,
      readOnly: false,
      ...config,
    };
    this.s3 = new BrandedS3Client(
      new s3.S3Client({
        region: config.s3Region,
        endpoint: config.overrides?.endpointUrl,
        credentials: config.overrides?.credentials,
        forcePathStyle: config.overrides?.forcePathStyle,
      })
    );
    this.logKey = S3KeyPaths.forLog(config.s3Prefix);
    this.valueDecoder = valueDecoder;
    this.valueEncoder = valueEncoder;
  }

  /**
   * Write new entry (two-phase: pre-flight repair, log, key object).
   */
  async set(key: K, value: V): Promise<Entry<K, V>> {
    if (this.config.readOnly === true) {
      throw new ReadOnlyError('Cannot call set() in read-only mode');
    }

    const maxRetries = 10;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      // Pre-flight: Repair
      const result = await this.getLatestAndRepair();
      const { logEtag, prevVersionId, prevHash, sequence, canWrite, orphanStatus } = result;

      if (canWrite !== undefined) {
        this.canWrite = canWrite;
      }
      if (orphanStatus !== undefined) {
        this.latestOrphanStatus = orphanStatus;
      }
      this.lastRepairCheckMs = Date.now();

      // Phase 1: Write to log
      const keyPath = S3KeyPaths.forKey(this.config.s3Prefix, key);
      let currentKeyEtag: KeyObjectETag<K> | undefined;

      try {
        const headResponse = await this.s3.headObject({
          Bucket: this.config.s3Bucket,
          Key: keyPath,
        });
        currentKeyEtag = HeadObjectCommandOutputs.keyObjectEtag(headResponse);
      } catch (error: any) {
        if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
          currentKeyEtag = undefined;
        } else {
          throw error;
        }
      }

      const newSequence =
        sequence !== undefined ? sequenceNext<K>(sequence) : sequenceFromJson<K>(0);
      const timestampMs = timestampNow<K>();

      // Encode value to JSON
      const encodedValue = this.valueEncoder(value);

      const entryForHash: LogEntryForHash<K, JSONValue> = {
        sequence: newSequence,
        key,
        value: encodedValue,
        timestampMs,
        previousHash: prevHash,
      };
      const entryHash = this.calculateHash(entryForHash);

      const logEntry = {
        sequence: newSequence,
        key,
        value: encodedValue,
        timestamp_ms: timestampMs,
        previous_version_id: prevVersionId,
        previous_hash: prevHash,
        hash: entryHash,
        previous_key_object_etag: currentKeyEtag ?? undefined,
      };

      try {
        const putResponse = await this.s3.putObject({
          Bucket: this.config.s3Bucket,
          Key: this.logKey,
          Body: stringifyCanonical(logEntry as JSONValue),
          ContentType: 'application/json',
          ...(logEtag !== undefined ? { IfMatch: logEtag } : { IfNoneMatch: '*' }),
        });

        const newLogVersionId = PutObjectCommandOutputs.logVersionId<K>(putResponse);
        if (newLogVersionId === undefined) {
          throw new Error(
            'S3 PutObject response missing VersionId - ensure versioning is enabled on bucket'
          );
        }

        // Phase 2: Write key object
        const keyData = {
          sequence: newSequence,
          key,
          value: encodedValue,
          timestamp_ms: timestampMs,
          log_version_id: newLogVersionId,
          hash: entryHash,
          previous_hash: prevHash,
        };

        let keyObjectEtag: KeyObjectETag<K> | undefined;
        try {
          const keyResponse = await this.s3.putObject({
            Bucket: this.config.s3Bucket,
            Key: keyPath,
            Body: stringifyCanonical(keyData as JSONValue),
            ContentType: 'application/json',
            ...(currentKeyEtag !== undefined ? { IfMatch: currentKeyEtag } : { IfNoneMatch: '*' }),
          });
          keyObjectEtag = PutObjectCommandOutputs.keyObjectEtag(keyResponse);
        } catch (error) {
          console.warn(
            `Failed to write key object for ${key} (log version ${newLogVersionId}):`,
            error
          );
        }

        return {
          key,
          value,
          timestampMs,
          versionId: newLogVersionId,
          sequence: newSequence,
          previousVersionId: prevVersionId,
          hash: entryHash,
          previousHash: prevHash,
          previousKeyObjectEtag: keyObjectEtag,
        };
      } catch (error: any) {
        if (error.name === 'PreconditionFailed' || error.$metadata?.httpStatusCode === 412) {
          continue;
        }
        throw error;
      }
    }

    throw new Error(`Failed to write log after ${maxRetries} retries`);
  }

  /**
   * Get latest value for key (with conditional orphan check and fallback).
   */
  async get(key: K): Promise<Entry<K, V>> {
    const currentTimeMs = Date.now();
    const timeSinceLastCheck = currentTimeMs - this.lastRepairCheckMs;

    if (timeSinceLastCheck >= this.config.repairCheckIntervalMs!) {
      if (!(this.canWrite === false || this.config.readOnly === true)) {
        const result = await this.getLatestAndRepair();
        if (result.canWrite !== undefined) {
          this.canWrite = result.canWrite;
        }
        if (result.orphanStatus !== undefined) {
          this.latestOrphanStatus = result.orphanStatus;
        }
        this.lastRepairCheckMs = currentTimeMs;
      }
    }

    const keyPath = S3KeyPaths.forKey(this.config.s3Prefix, key);
    try {
      const response = await this.s3.getObject({
        Bucket: this.config.s3Bucket,
        Key: keyPath,
      });

      const data = await readBodyAsJson(response.Body);

      return {
        key: data.key as K,
        value: this.valueDecoder(data.value),
        timestampMs: timestampFromJson(data.timestamp_ms as number),
        versionId: data.log_version_id as LogVersionId<K>,
        sequence: sequenceFromJson(data.sequence as number),
        previousVersionId: undefined,
        hash: hashFromJson(data.hash as string),
        previousHash: hashFromJson(data.previous_hash as string),
        previousKeyObjectEtag: undefined,
      };
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        if (
          this.latestOrphanStatus?.isOrphaned === true &&
          this.latestOrphanStatus.orphanKey === key &&
          this.latestOrphanStatus.orphanEntry !== undefined &&
          (this.canWrite === false || this.config.readOnly === true)
        ) {
          return this.latestOrphanStatus.orphanEntry;
        }
        throw new KeyNotFoundError(`Key '${key}' not found`);
      }
      throw error;
    }
  }

  /**
   * Get specific log version by S3 version ID.
   */
  async getLogVersion(versionId: LogVersionId<K>): Promise<Entry<K, V>> {
    try {
      const response = await this.s3.getObject({
        Bucket: this.config.s3Bucket,
        Key: this.logKey,
        VersionId: versionId,
      });

      const data = await readBodyAsJson(response.Body);

      return {
        key: data.key as K,
        value: this.valueDecoder(data.value),
        timestampMs: timestampFromJson(data.timestamp_ms as number),
        versionId,
        sequence: sequenceFromJson(data.sequence as number),
        previousVersionId:
          data.previous_version_id !== undefined
            ? (data.previous_version_id as LogVersionId<K>)
            : undefined,
        hash: hashFromJson(data.hash as string),
        previousHash: hashFromJson(data.previous_hash as string),
        previousKeyObjectEtag:
          data.previous_key_object_etag !== undefined
            ? (data.previous_key_object_etag as KeyObjectETag<K>)
            : undefined,
      };
    } catch (error: any) {
      if (
        error.name === 'NoSuchKey' ||
        error.name === 'NoSuchVersion' ||
        error.$metadata?.httpStatusCode === 404
      ) {
        throw new KeyNotFoundError(`Log version '${versionId}' not found`);
      }
      throw error;
    }
  }

  /**
   * Get all entries for a key (descending order - newest first).
   */
  async history(
    key: K,
    beforeVersionId: KeyVersionId<K> | undefined,
    limit: number | undefined
  ): Promise<[Entry<K, V>[], KeyVersionId<K> | undefined]> {
    const keyPath = S3KeyPaths.forKey(this.config.s3Prefix, key);
    const entries: Entry<K, V>[] = [];

    let prependOrphan = false;
    if (
      beforeVersionId === undefined &&
      this.latestOrphanStatus?.isOrphaned === true &&
      this.latestOrphanStatus.orphanKey === key &&
      this.latestOrphanStatus.orphanEntry !== undefined
    ) {
      prependOrphan = true;
      entries.push(this.latestOrphanStatus.orphanEntry);
    }

    let lastKeyVersionId: KeyVersionId<K> | undefined = undefined;
    try {
      let versionIdMarker = beforeVersionId ?? undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.listObjectVersions({
          Bucket: this.config.s3Bucket,
          Prefix: keyPath,
          KeyMarker: versionIdMarker !== undefined ? keyPath : undefined,
          VersionIdMarker: versionIdMarker,
        });

        const versions = response.Versions ?? [];
        for (const version of versions) {
          if (version.Key !== keyPath) continue;
          if (beforeVersionId !== undefined && version.VersionId === beforeVersionId) continue;

          const keyVersionId = ObjectVersions.keyVersionId<K>(version);
          const objResponse = await this.s3.getObject({
            Bucket: this.config.s3Bucket,
            Key: keyPath,
            VersionId: keyVersionId,
          });

          const data = await readBodyAsJson(objResponse.Body);

          const entry: Entry<K, V> = {
            key: data.key as K,
            value: this.valueDecoder(data.value),
            timestampMs: timestampFromJson(data.timestamp_ms as number),
            versionId: data.log_version_id as LogVersionId<K>,
            sequence: sequenceFromJson(data.sequence as number),
            previousVersionId: undefined,
            hash: hashFromJson(data.hash as string),
            previousHash: hashFromJson(data.previous_hash as string),
            previousKeyObjectEtag: undefined,
          };
          entries.push(entry);
          lastKeyVersionId = keyVersionId;

          if (limit !== undefined && entries.length >= limit) {
            return [entries, lastKeyVersionId];
          }
        }

        isTruncated = response.IsTruncated ?? false;
        versionIdMarker =
          response.NextVersionIdMarker !== undefined
            ? (response.NextVersionIdMarker as KeyVersionId<K>)
            : undefined;
      }
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        if (prependOrphan) {
          return [entries, undefined];
        }
        return [[], undefined];
      }
      throw error;
    }

    const oldestKeyVersionId: KeyVersionId<K> | undefined =
      entries.length > 0 && !prependOrphan ? lastKeyVersionId : undefined;
    return [entries, oldestKeyVersionId];
  }

  /**
   * Get entries from global log (descending order - newest first).
   */
  async logEntries(
    beforeVersionId: LogVersionId<K> | undefined,
    limit: number | undefined
  ): Promise<Entry<K, V>[]> {
    const entries: Entry<K, V>[] = [];

    try {
      let versionIdMarker = beforeVersionId ?? undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.listObjectVersions({
          Bucket: this.config.s3Bucket,
          Prefix: this.logKey,
          KeyMarker: versionIdMarker !== undefined ? this.logKey : undefined,
          VersionIdMarker: versionIdMarker,
        });

        const versions = response.Versions ?? [];
        for (const version of versions) {
          if (version.Key !== this.logKey) continue;
          if (beforeVersionId !== undefined && version.VersionId === beforeVersionId) continue;

          const objResponse = await this.s3.getObject({
            Bucket: this.config.s3Bucket,
            Key: this.logKey,
            VersionId: version.VersionId,
          });

          const data = await readBodyAsJson(objResponse.Body);

          const logVersionId = ObjectVersions.logVersionId<K>(version);

          const entry: Entry<K, V> = {
            key: data.key as K,
            value: this.valueDecoder(data.value),
            timestampMs: timestampFromJson(data.timestamp_ms as number),
            versionId: logVersionId,
            sequence: sequenceFromJson(data.sequence as number),
            previousVersionId:
              data.previous_version_id !== undefined
                ? (data.previous_version_id as LogVersionId<K>)
                : undefined,
            hash: hashFromJson(data.hash as string),
            previousHash: hashFromJson(data.previous_hash as string),
            previousKeyObjectEtag:
              data.previous_key_object_etag !== undefined
                ? (data.previous_key_object_etag as KeyObjectETag<K>)
                : undefined,
          };
          entries.push(entry);

          if (limit !== undefined && entries.length >= limit) {
            return entries;
          }
        }

        isTruncated = response.IsTruncated ?? false;
        versionIdMarker =
          response.NextVersionIdMarker !== undefined
            ? (response.NextVersionIdMarker as LogVersionId<K>)
            : undefined;
      }
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        return [];
      }
      throw error;
    }

    return entries;
  }

  /**
   * List all keys in the system (lexicographic order).
   */
  async listKeys(afterKey: K | undefined, limit: number | undefined): Promise<K[]> {
    const keys: K[] = [];
    const prefix = `${this.config.s3Prefix}keys/`;

    try {
      let continuationToken: string | undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.listObjectsV2({
          Bucket: this.config.s3Bucket,
          Prefix: prefix,
          StartAfter: afterKey !== undefined ? `${prefix}${afterKey}.json` : prefix,
          ContinuationToken: continuationToken,
        });

        const contents = response.Contents ?? [];
        for (const obj of contents) {
          const keyName = obj.Key.substring(prefix.length);
          if (keyName.endsWith('.json')) {
            const cleanKey = keyName.substring(0, keyName.length - 5) as K;
            keys.push(cleanKey);

            if (limit !== undefined && keys.length >= limit) {
              return keys;
            }
          }
        }

        isTruncated = response.IsTruncated;
        continuationToken = response.NextContinuationToken;
      }
    } catch (error) {
      return [];
    }

    return keys;
  }

  /**
   * Verify single entry integrity.
   */
  async verify(entry: Entry<K, V>): Promise<boolean> {
    // Encode value back to JSON for hash verification
    const entryForHash: LogEntryForHash<K, JSONValue> = {
      sequence: entry.sequence,
      key: entry.key,
      value: this.valueEncoder(entry.value),
      timestampMs: entry.timestampMs,
      previousHash: entry.previousHash,
    };
    const expectedHash = this.calculateHash(entryForHash);
    return entry.hash === expectedHash;
  }

  /**
   * Verify hash chain in log.
   */
  async verifyLogChain(limit?: number): Promise<boolean> {
    const entries = await this.logEntries(undefined, limit ?? undefined);

    if (entries.length === 0) {
      return true;
    }

    for (const entry of entries) {
      if (!(await this.verify(entry))) {
        console.error(`Hash verification failed for entry ${entry.sequence}`);
        return false;
      }
    }

    for (let i = 0; i < entries.length - 1; i++) {
      const current = entries[i];
      const previous = entries[i + 1];

      if (current.previousHash !== previous.hash) {
        console.error(`Chain broken between entry ${current.sequence} and ${previous.sequence}`);
        return false;
      }
    }

    return true;
  }

  /**
   * Close client and cleanup resources.
   */
  async close(): Promise<void> {
    this.s3.client.destroy();
  }

  // Private helper methods

  private calculateHash(entryForHash: LogEntryForHash<K, JSONValue>): Hash<K> {
    return hashCompute(entryForHash);
  }

  private async getLatestAndRepair(): Promise<{
    logEtag?: string;
    prevVersionId?: LogVersionId<K>;
    prevHash: Hash<K>;
    sequence?: Sequence<K>;
    canWrite?: boolean;
    orphanStatus?: OrphanStatus<K, V>;
  }> {
    try {
      const response = await this.s3.getObject({
        Bucket: this.config.s3Bucket,
        Key: this.logKey,
      });

      const logEtag = response.ETag;
      const currentVersionId = response.VersionId as LogVersionId<K>;
      const data = await readBodyAsJson(response.Body);

      const latestEntry: Entry<K, V> = {
        key: data.key as K,
        value: this.valueDecoder(data.value),
        timestampMs: timestampFromJson(data.timestamp_ms as number),
        versionId: currentVersionId,
        sequence: sequenceFromJson(data.sequence as number),
        previousVersionId:
          data.previous_version_id !== undefined
            ? (data.previous_version_id as LogVersionId<K>)
            : undefined,
        hash: hashFromJson(data.hash as string),
        previousHash: hashFromJson(data.previous_hash as string),
        previousKeyObjectEtag:
          data.previous_key_object_etag !== undefined
            ? (data.previous_key_object_etag as KeyObjectETag<K>)
            : undefined,
      };

      const [canWrite, orphanStatus] = await this.repairOrphan(latestEntry);

      return {
        logEtag,
        prevVersionId: currentVersionId,
        prevHash: hashFromJson<K>(data.hash as string),
        sequence: sequenceFromJson<K>(data.sequence as number),
        canWrite,
        orphanStatus,
      };
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        return {
          prevHash: hashGenesis<K>(),
          sequence: sequenceInitial<K>(),
        };
      }
      throw error;
    }
  }

  private async repairOrphan(
    latestLog: Entry<K, V>
  ): Promise<[boolean | undefined, OrphanStatus<K, V> | undefined]> {
    if (this.config.readOnly === true || this.canWrite === false) {
      const keyPath = S3KeyPaths.forKey(this.config.s3Prefix, latestLog.key);
      try {
        await this.s3.headObject({
          Bucket: this.config.s3Bucket,
          Key: keyPath,
        });
        return [
          this.canWrite,
          {
            isOrphaned: false,
            orphanKey: undefined,
            orphanEntry: undefined,
            checkedAt: Date.now(),
          },
        ];
      } catch (error: any) {
        if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
          return [
            false,
            {
              isOrphaned: true,
              orphanKey: latestLog.key,
              orphanEntry: latestLog,
              checkedAt: Date.now(),
            },
          ];
        }
        throw error;
      }
    }

    const currentTimeMs = Date.now();
    const keyPath = S3KeyPaths.forKey(this.config.s3Prefix, latestLog.key);

    // Encode value back to JSON for repair
    const repairData = {
      sequence: latestLog.sequence,
      key: latestLog.key,
      value: this.valueEncoder(latestLog.value),
      timestamp_ms: latestLog.timestampMs,
      log_version_id: latestLog.versionId,
      hash: latestLog.hash,
      previous_hash: latestLog.previousHash,
    };

    try {
      await this.s3.putObject({
        Bucket: this.config.s3Bucket,
        Key: keyPath,
        Body: stringifyCanonical(repairData as JSONValue),
        ContentType: 'application/json',
        ...(latestLog.previousKeyObjectEtag !== undefined
          ? { IfMatch: latestLog.previousKeyObjectEtag }
          : { IfNoneMatch: '*' }),
      });

      return [
        true,
        {
          isOrphaned: false,
          orphanKey: undefined,
          orphanEntry: undefined,
          checkedAt: currentTimeMs,
        },
      ];
    } catch (error: any) {
      if (error.name === 'PreconditionFailed' || error.$metadata?.httpStatusCode === 412) {
        return [
          true,
          {
            isOrphaned: false,
            orphanKey: undefined,
            orphanEntry: undefined,
            checkedAt: currentTimeMs,
          },
        ];
      } else if (
        error.name === 'AccessDenied' ||
        error.name === 'Forbidden' ||
        error.$metadata?.httpStatusCode === 403
      ) {
        console.log('Read-only mode detected - orphan repair disabled');
        return [
          false,
          {
            isOrphaned: true,
            orphanKey: latestLog.key,
            orphanEntry: latestLog,
            checkedAt: currentTimeMs,
          },
        ];
      } else {
        console.warn('Pre-flight repair failed:', error);
        return [undefined, undefined];
      }
    }
  }
}
