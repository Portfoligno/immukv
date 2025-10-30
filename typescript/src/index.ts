/**
 * ImmuKV - Lightweight immutable key-value store using S3 versioning.
 */

export { ImmuKVClient } from './client';
export {
  Config,
  Entry,
  LogEntryForHash,
  OrphanStatus,
  KeyNotFoundError,
  ReadOnlyError,
} from './types';

export const VERSION = '0.1.0-dev';
