# ImmuKV - TypeScript Client

Lightweight immutable key-value store using S3 versioning.

## Installation

```bash
npm install immukv
```

## Quick Start

```typescript
import { ImmuKVClient, Config } from 'immukv';

const config: Config = {
  s3Bucket: 'your-bucket',
  s3Region: 'us-east-1',
  s3Prefix: '',
};

// Identity functions for JSON values (use custom encoders/decoders for complex types)
const identity = <T>(x: T): T => x;

const client = new ImmuKVClient(config, identity, identity);

// Write
const entry = await client.set('key1', { value: 'data' });
console.log(`Committed: ${entry.versionId}`);

// Read
const latest = await client.get('key1');
console.log('Latest:', latest.value);

await client.close();
```

## Features

- **Immutable log** - All writes append to global log
- **Fast reads** - Single S3 request for latest value
- **Hash chain** - Cryptographic integrity verification
- **No database** - Uses S3 versioning only
- **Auto-repair** - Orphaned entries repaired automatically

See the [full documentation](../README.md) for more details.
