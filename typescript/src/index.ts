/**
 * ImmuKV - Lightweight immutable key-value store using S3 versioning.
 */

import packageJson from '../package.json';

export { ImmuKVClient } from './client';
export { JSONValue, ValueParser } from './jsonHelpers';
export {
  Config,
  Entry,
  LogEntryForHash,
  OrphanStatus,
  KeyNotFoundError,
  ReadOnlyError,
} from './types';

export const VERSION = packageJson.version;
