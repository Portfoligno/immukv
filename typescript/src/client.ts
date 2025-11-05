/**
 * ImmuKV client implementation.
 */

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  ListObjectVersionsCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { createHash } from 'crypto';
import {
  Config,
  Entry,
  Hash,
  KeyNotFoundError,
  KeyObjectETag,
  KeyVersionId,
  LogEntryForHash,
  LogVersionId,
  OrphanStatus,
  ReadOnlyError,
  Sequence,
  TimestampMs,
} from './types';
import { JSONValue, ValueParser, stringifyCanonical } from './jsonHelpers';
import { readBodyAsJson } from './s3Helpers';

/**
 * Main client interface - Simple S3 versioning with auto-repair.
 */
export class ImmuKVClient<K extends string = string, V = any> {
  private config: Config;
  private s3: S3Client;
  private logKey: string;
  private valueParser: ValueParser<V>;
  private lastRepairCheckMs: number = 0;
  private canWrite?: boolean;
  private latestOrphanStatus?: OrphanStatus<K, V>;

  constructor(config: Config, valueParser: ValueParser<V>) {
    this.config = {
      repairCheckIntervalMs: 300000,
      readOnly: false,
      ...config,
    };
    this.s3 = new S3Client({ region: config.s3Region });
    this.logKey = `${config.s3Prefix}_log.json`;
    this.valueParser = valueParser;
  }

  /**
   * Write new entry (two-phase: pre-flight repair, log, key object).
   */
  async set(key: K, value: V): Promise<Entry<K, V>> {
    if (this.config.readOnly) {
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
      if (orphanStatus) {
        this.latestOrphanStatus = orphanStatus;
      }
      this.lastRepairCheckMs = Date.now();

      // Phase 1: Write to log
      const keyPath = `${this.config.s3Prefix}keys/${key}.json`;
      let currentKeyEtag: KeyObjectETag<K> | undefined;

      try {
        const headResponse = await this.s3.send(
          new HeadObjectCommand({
            Bucket: this.config.s3Bucket,
            Key: keyPath,
          })
        );
        currentKeyEtag = headResponse.ETag as KeyObjectETag<K>;
      } catch (error: any) {
        if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
          currentKeyEtag = undefined;
        } else {
          throw error;
        }
      }

      const newSequence = ((sequence ?? 0) + 1) as Sequence<K>;
      const timestampMs = Date.now() as TimestampMs<K>;

      const entryForHash: LogEntryForHash<K, V> = {
        sequence: newSequence,
        key,
        value,
        timestampMs,
        previousHash: prevHash,
      };
      const entryHash = this.calculateHash(entryForHash);

      const logEntry = {
        sequence: newSequence,
        key,
        value,
        timestamp_ms: timestampMs,
        previous_version_id: prevVersionId,
        previous_hash: prevHash,
        hash: entryHash,
        previous_key_object_etag: currentKeyEtag,
      };

      try {
        const putResponse = await this.s3.send(
          new PutObjectCommand({
            Bucket: this.config.s3Bucket,
            Key: this.logKey,
            Body: stringifyCanonical(logEntry as JSONValue),
            ContentType: 'application/json',
            ...(logEtag ? { IfMatch: logEtag } : { IfNoneMatch: '*' }),
          })
        );

        const newLogVersionId = putResponse.VersionId as LogVersionId<K>;

        // Phase 2: Write key object
        const keyData = {
          sequence: newSequence,
          key,
          value,
          timestamp_ms: timestampMs,
          log_version_id: newLogVersionId,
          hash: entryHash,
          previous_hash: prevHash,
        };

        let keyObjectEtag: KeyObjectETag<K> | undefined;
        try {
          const keyResponse = await this.s3.send(
            new PutObjectCommand({
              Bucket: this.config.s3Bucket,
              Key: keyPath,
              Body: stringifyCanonical(keyData as JSONValue),
              ContentType: 'application/json',
              ...(currentKeyEtag ? { IfMatch: currentKeyEtag } : { IfNoneMatch: '*' }),
            })
          );
          keyObjectEtag = keyResponse.ETag as KeyObjectETag<K>;
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
      if (!(this.canWrite === false || this.config.readOnly)) {
        const result = await this.getLatestAndRepair();
        if (result.canWrite !== undefined) {
          this.canWrite = result.canWrite;
        }
        if (result.orphanStatus) {
          this.latestOrphanStatus = result.orphanStatus;
        }
        this.lastRepairCheckMs = currentTimeMs;
      }
    }

    const keyPath = `${this.config.s3Prefix}keys/${key}.json`;
    try {
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.config.s3Bucket,
          Key: keyPath,
        })
      );

      const data = await readBodyAsJson(response.Body);

      return {
        key: data.key as K,
        value: this.valueParser(data.value),
        timestampMs: data.timestamp_ms as TimestampMs<K>,
        versionId: data.log_version_id as LogVersionId<K>,
        sequence: data.sequence as Sequence<K>,
        previousVersionId: undefined,
        hash: data.hash as Hash<K>,
        previousHash: data.previous_hash as Hash<K>,
        previousKeyObjectEtag: undefined,
      };
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        if (
          this.latestOrphanStatus?.isOrphaned &&
          this.latestOrphanStatus.orphanKey === key &&
          this.latestOrphanStatus.orphanEntry &&
          (this.canWrite === false || this.config.readOnly)
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
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.config.s3Bucket,
          Key: this.logKey,
          VersionId: versionId,
        })
      );

      const data = await readBodyAsJson(response.Body);

      return {
        key: data.key as K,
        value: this.valueParser(data.value),
        timestampMs: data.timestamp_ms as TimestampMs<K>,
        versionId,
        sequence: data.sequence as Sequence<K>,
        previousVersionId: data.previous_version_id as LogVersionId<K> | undefined,
        hash: data.hash as Hash<K>,
        previousHash: data.previous_hash as Hash<K>,
        previousKeyObjectEtag: data.previous_key_object_etag as KeyObjectETag<K> | undefined,
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
    beforeVersionId: KeyVersionId<K> | null,
    limit: number | null
  ): Promise<[Entry<K, V>[], KeyVersionId<K> | null]> {
    const keyPath = `${this.config.s3Prefix}keys/${key}.json`;
    const entries: Entry<K, V>[] = [];

    let prependOrphan = false;
    if (
      beforeVersionId === null &&
      this.latestOrphanStatus?.isOrphaned &&
      this.latestOrphanStatus.orphanKey === key &&
      this.latestOrphanStatus.orphanEntry
    ) {
      prependOrphan = true;
      entries.push(this.latestOrphanStatus.orphanEntry);
    }

    let lastKeyVersionId: KeyVersionId<K> | null = null;
    try {
      let versionIdMarker = beforeVersionId ?? undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.send(
          new ListObjectVersionsCommand({
            Bucket: this.config.s3Bucket,
            Prefix: keyPath,
            KeyMarker: keyPath,
            VersionIdMarker: versionIdMarker,
          })
        );

        const versions = response.Versions ?? [];
        for (const version of versions) {
          if (version.Key !== keyPath) continue;
          if (beforeVersionId && version.VersionId === beforeVersionId) continue;

          const keyVersionId = version.VersionId as KeyVersionId<K>;
          const objResponse = await this.s3.send(
            new GetObjectCommand({
              Bucket: this.config.s3Bucket,
              Key: keyPath,
              VersionId: keyVersionId,
            })
          );

          const data = await readBodyAsJson(objResponse.Body);

          const entry: Entry<K, V> = {
            key: data.key as K,
            value: this.valueParser(data.value),
            timestampMs: data.timestamp_ms as TimestampMs<K>,
            versionId: data.log_version_id as LogVersionId<K>,
            sequence: data.sequence as Sequence<K>,
            previousVersionId: undefined,
            hash: data.hash as Hash<K>,
            previousHash: data.previous_hash as Hash<K>,
            previousKeyObjectEtag: undefined,
          };
          entries.push(entry);
          lastKeyVersionId = keyVersionId;

          if (limit !== null && entries.length >= limit) {
            return [entries, lastKeyVersionId];
          }
        }

        isTruncated = response.IsTruncated ?? false;
        versionIdMarker = response.NextVersionIdMarker as KeyVersionId<K> | undefined;
      }
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        if (prependOrphan) {
          return [entries, null];
        }
        return [[], null];
      }
      throw error;
    }

    const oldestKeyVersionId: KeyVersionId<K> | null =
      entries.length > 0 && !prependOrphan ? lastKeyVersionId : null;
    return [entries, oldestKeyVersionId];
  }

  /**
   * Get entries from global log (descending order - newest first).
   */
  async logEntries(
    beforeVersionId: LogVersionId<K> | null,
    limit: number | null
  ): Promise<Entry<K, V>[]> {
    const entries: Entry<K, V>[] = [];

    try {
      let versionIdMarker = beforeVersionId ?? undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.send(
          new ListObjectVersionsCommand({
            Bucket: this.config.s3Bucket,
            Prefix: this.logKey,
            KeyMarker: this.logKey,
            VersionIdMarker: versionIdMarker,
          })
        );

        const versions = response.Versions ?? [];
        for (const version of versions) {
          if (version.Key !== this.logKey) continue;
          if (beforeVersionId && version.VersionId === beforeVersionId) continue;

          const objResponse = await this.s3.send(
            new GetObjectCommand({
              Bucket: this.config.s3Bucket,
              Key: this.logKey,
              VersionId: version.VersionId,
            })
          );

          const data = await readBodyAsJson(objResponse.Body);

          const entry: Entry<K, V> = {
            key: data.key as K,
            value: this.valueParser(data.value),
            timestampMs: data.timestamp_ms as TimestampMs<K>,
            versionId: version.VersionId as LogVersionId<K>,
            sequence: data.sequence as Sequence<K>,
            previousVersionId: data.previous_version_id as LogVersionId<K> | undefined,
            hash: data.hash as Hash<K>,
            previousHash: data.previous_hash as Hash<K>,
            previousKeyObjectEtag: data.previous_key_object_etag as KeyObjectETag<K> | undefined,
          };
          entries.push(entry);

          if (limit !== null && entries.length >= limit) {
            return entries;
          }
        }

        isTruncated = response.IsTruncated ?? false;
        versionIdMarker = response.NextVersionIdMarker as LogVersionId<K> | undefined;
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
  async listKeys(afterKey: K | null, limit: number | null): Promise<K[]> {
    const keys: K[] = [];
    const prefix = `${this.config.s3Prefix}keys/`;

    try {
      let continuationToken: string | undefined;
      let isTruncated = true;

      while (isTruncated) {
        const response = await this.s3.send(
          new ListObjectsV2Command({
            Bucket: this.config.s3Bucket,
            Prefix: prefix,
            StartAfter: afterKey ? `${prefix}${afterKey}.json` : prefix,
            ContinuationToken: continuationToken,
          })
        );

        const contents = response.Contents ?? [];
        for (const obj of contents) {
          const keyName = obj.Key!.substring(prefix.length);
          if (keyName.endsWith('.json')) {
            const cleanKey = keyName.substring(0, keyName.length - 5) as K;
            keys.push(cleanKey);

            if (limit !== null && keys.length >= limit) {
              return keys;
            }
          }
        }

        isTruncated = response.IsTruncated ?? false;
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
    const entryForHash: LogEntryForHash<K, V> = {
      sequence: entry.sequence,
      key: entry.key,
      value: entry.value,
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
    const entries = await this.logEntries(null, limit ?? null);

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
    this.s3.destroy();
  }

  // Private helper methods

  private calculateHash(entryForHash: LogEntryForHash<K, V>): Hash<K> {
    // Serialize value with sorted keys (canonical JSON)
    const valueJson = JSON.stringify(entryForHash.value, (key, val) => {
      if (val && typeof val === 'object' && !Array.isArray(val)) {
        return Object.keys(val)
          .sort()
          .reduce((sorted: { [key: string]: JSONValue }, key: string) => {
            sorted[key] = val[key];
            return sorted;
          }, {});
      }
      return val;
    });
    const canonical = `${entryForHash.sequence}|${entryForHash.key}|${valueJson}|${entryForHash.timestampMs}|${entryForHash.previousHash}`;
    const hashBytes = createHash('sha256').update(canonical, 'utf8').digest('hex');
    return `sha256:${hashBytes}` as Hash<K>;
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
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.config.s3Bucket,
          Key: this.logKey,
        })
      );

      const logEtag = response.ETag;
      const currentVersionId = response.VersionId as LogVersionId<K>;
      const data = await readBodyAsJson(response.Body);

      const latestEntry: Entry<K, V> = {
        key: data.key as K,
        value: this.valueParser(data.value),
        timestampMs: data.timestamp_ms as TimestampMs<K>,
        versionId: currentVersionId,
        sequence: data.sequence as Sequence<K>,
        previousVersionId: data.previous_version_id as LogVersionId<K> | undefined,
        hash: data.hash as Hash<K>,
        previousHash: data.previous_hash as Hash<K>,
        previousKeyObjectEtag: data.previous_key_object_etag as KeyObjectETag<K> | undefined,
      };

      const [canWrite, orphanStatus] = await this.repairOrphan(latestEntry);

      return {
        logEtag,
        prevVersionId: currentVersionId,
        prevHash: data.hash as Hash<K>,
        sequence: data.sequence as Sequence<K>,
        canWrite,
        orphanStatus,
      };
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        return {
          prevHash: 'sha256:genesis' as Hash<K>,
          sequence: -1 as Sequence<K>,
        };
      }
      throw error;
    }
  }

  private async repairOrphan(
    latestLog: Entry<K, V>
  ): Promise<[boolean | undefined, OrphanStatus<K, V> | undefined]> {
    if (this.config.readOnly || this.canWrite === false) {
      const keyPath = `${this.config.s3Prefix}keys/${latestLog.key}.json`;
      try {
        await this.s3.send(
          new HeadObjectCommand({
            Bucket: this.config.s3Bucket,
            Key: keyPath,
          })
        );
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
    const keyPath = `${this.config.s3Prefix}keys/${latestLog.key}.json`;

    const repairData = {
      sequence: latestLog.sequence,
      key: latestLog.key,
      value: latestLog.value,
      timestamp_ms: latestLog.timestampMs,
      log_version_id: latestLog.versionId,
      hash: latestLog.hash,
      previous_hash: latestLog.previousHash,
    };

    try {
      await this.s3.send(
        new PutObjectCommand({
          Bucket: this.config.s3Bucket,
          Key: keyPath,
          Body: stringifyCanonical(repairData as JSONValue),
          ContentType: 'application/json',
          ...(latestLog.previousKeyObjectEtag
            ? { IfMatch: latestLog.previousKeyObjectEtag }
            : { IfNoneMatch: '*' }),
        })
      );

      console.log(`Propagated log entry to key object for ${latestLog.key}`);
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
