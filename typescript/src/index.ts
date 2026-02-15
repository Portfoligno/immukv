/**
 * ImmuKV - Lightweight immutable key-value store using S3 versioning.
 */

export { ImmuKVClient } from './client';
export type { JSONValue, ValueDecoder, ValueEncoder } from './jsonHelpers';
export {
  KeyNotFoundError,
  ReadOnlyError,
  type Config,
  type CredentialProvider,
  type Entry,
  type Hash,
  type KeyObjectETag,
  type KeyVersionId,
  type LogVersionId,
  type Sequence,
  type StaticCredentials,
  type TimestampMs,
} from './types';

export const VERSION = '__VERSION_EeEyfbyVyf4JmFfk__';
