/**
 * ImmuKV - Lightweight immutable key-value store using S3 versioning.
 */

export { ImmuKVClient } from './client';
export { JSONValue, ValueParser } from './jsonHelpers';
export { Config, Entry, KeyNotFoundError, ReadOnlyError } from './types';

export const VERSION = '__VERSION_EeEyfbyVyf4JmFfk__';
