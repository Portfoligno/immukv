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

// List keys
const keys = await client.listKeys(undefined, 100);

// List keys with prefix filtering (server-side)
const sensorKeys = await client.listKeysWithPrefix('sensor-', undefined, 100);

await client.close();
```

## Features

- **Immutable log** - All writes append to global log
- **Fast reads** - Single S3 request for latest value
- **Hash chain** - Cryptographic integrity verification
- **No database** - Uses S3 versioning only
- **Auto-repair** - Orphaned entries repaired automatically
- **Credential providers** - Pluggable async credential refresh via `CredentialProvider`

## Credential Providers

The client supports static credentials or an async credential provider for dynamic credential refresh (e.g., OIDC federation).

```typescript
import { Config, CredentialProvider, StaticCredentials } from 'immukv';

// Static credentials
const config: Config = {
  s3Bucket: 'bucket',
  s3Region: 'us-east-1',
  s3Prefix: '',
  overrides: {
    credentials: {
      accessKeyId: 'AKIA...',
      secretAccessKey: '...',
      sessionToken: '...',
    },
  },
};

// Async credential provider
const provider: CredentialProvider = async () => ({
  accessKeyId: 'AKIA...',
  secretAccessKey: '...',
  sessionToken: '...',
});

const configWithProvider: Config = {
  s3Bucket: 'bucket',
  s3Region: 'us-east-1',
  s3Prefix: '',
  overrides: { credentials: provider },
};
```

See the [full documentation](../README.md) for more details.
