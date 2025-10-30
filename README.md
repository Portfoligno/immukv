# ImmuKV

**Lightweight immutable key-value store using S3 versioning**

ImmuKV is a simple, serverless immutable key-value store that uses only S3 versioning - no DynamoDB, no background jobs, no complex infrastructure.

## Design Philosophy

- **Maximum simplicity** - Just S3, no background repair jobs, no status tracking
- **Global ordering** - All changes recorded in versioned global log
- **Fast key access** - Single S3 read for latest value
- **Automatic orphan repair** - ETag-based conditional writes handle failures inline
- **Cryptographic integrity** - SHA-256 hash chain prevents tampering

## Key Features

### Core Architecture

- **Global log** (`_log.json`) - Single versioned object containing all changes
- **Key objects** (`keys/{key}.json`) - One versioned object per key for fast access
- **Two-phase writes** - Log first (never lost), then key object (may be orphaned temporarily)
- **Inline repair** - Orphaned entries automatically repaired during normal operations

### Trade-offs

**Gains:**
- Extreme simplicity (just S3, no background jobs)
- Fast key lookups (single S3 read)
- Lower cost (no DynamoDB)
- Lambda/serverless friendly

**Limitations:**
- Must read log versions sequentially (no random access by entry number)
- S3 version IDs are opaque strings (not sequential integers)
- Orphans exist temporarily (repaired within configurable interval, default 5 minutes)

## Installation

### Python

```bash
pip install immukv
```

### TypeScript

```bash
npm install immukv
```

## Quick Start

### Python

```python
from immukv import ImmuKVClient, Config

config = Config(
    s3_bucket="your-bucket",
    s3_region="us-east-1",
    s3_prefix=""
)

with ImmuKVClient(config) as client:
    # Write
    entry = client.set("sensor-012352", {"alpha": 0.15, "beta": 2.8})
    print(f"Committed: {entry.version_id}")

    # Read (single S3 request)
    latest = client.get("sensor-012352")
    print(f"Latest: {latest.value}")

    # History
    history, _ = client.history("sensor-012352", None, None)
    for entry in history:
        print(f"Seq {entry.sequence}: {entry.value}")
```

### TypeScript

```typescript
import { ImmuKVClient, Config } from 'immukv';

const config: Config = {
  s3Bucket: 'your-bucket',
  s3Region: 'us-east-1',
  s3Prefix: ''
};

const client = new ImmuKVClient(config);

// Write
const entry = await client.set('sensor-012352', { alpha: 0.15, beta: 2.8 });
console.log(`Committed: ${entry.versionId}`);

// Read (single S3 request)
const latest = await client.get('sensor-012352');
console.log('Latest:', latest.value);

await client.close();
```

## How It Works

### Two-Phase Write Protocol

Every write operation happens in two phases:

1. **Phase 1: Log Write** (always succeeds or throws)
   - Append entry to `_log.json` using optimistic locking
   - S3 creates new version automatically
   - Entry is now durable and will never be lost

2. **Phase 2: Key Object Write** (may fail temporarily)
   - Write/update `keys/{key}.json` with full entry data
   - If this fails, entry is "orphaned" (exists in log but not in key object)
   - Orphans are automatically repaired on next activity

### Automatic Orphan Repair

- **Pre-flight check**: Every write operation repairs any existing orphan first
- **Conditional reads**: Read operations check for orphans at configurable intervals
- **ETag-based repair**: Uses stored previous ETag for idempotent conditional writes
- **No background jobs**: All repair happens inline during normal operations

### Hash Chain Integrity

Each entry includes:
- `hash` - SHA-256 hash of entry data
- `previous_hash` - Hash from previous entry

This creates a tamper-evident chain where modifying any past entry breaks all subsequent hashes.

## S3 Setup

### Enable Versioning

```bash
aws s3api put-bucket-versioning \
  --bucket your-bucket \
  --versioning-configuration Status=Enabled
```

### Storage Layout

```
s3://your-bucket/
├── _log.json (versioned)
│   ├── Version: xxx (latest)
│   ├── Version: yyy
│   └── Version: zzz (first)
└── keys/
    ├── sensor-012352.json (versioned)
    ├── sensor-012353.json (versioned)
    └── ...
```

## API Reference

### Configuration

```python
# Python
config = Config(
    s3_bucket="bucket-name",
    s3_region="us-east-1",
    s3_prefix="",
    kms_key_id=None,  # Optional KMS encryption
    repair_check_interval_ms=300000,  # 5 minutes
    read_only=False  # Set True to disable writes
)
```

```typescript
// TypeScript
const config: Config = {
  s3Bucket: 'bucket-name',
  s3Region: 'us-east-1',
  s3Prefix: '',
  kmsKeyId?: string,  // Optional KMS encryption
  repairCheckIntervalMs: 300000,  // 5 minutes
  readOnly: false  // Set true to disable writes
};
```

### Core Operations

#### `set(key, value)` - Write Entry

Creates new immutable entry in log and key object.

```python
entry = client.set("key1", {"data": "value"})
```

#### `get(key)` - Read Latest

Retrieves latest value for key (single S3 read).

```python
entry = client.get("key1")
```

#### `history(key, before_version_id, limit)` - Get Key History

Retrieves all versions of a key (newest first).

```python
entries, oldest_version = client.history("key1", None, 10)
```

#### `log_entries(before_version_id, limit)` - Global Log

Retrieves entries from global log across all keys (newest first).

```python
entries = client.log_entries(None, 100)
```

#### `list_keys(after_key, limit)` - List Keys

Lists all keys in lexicographic order.

```python
keys = client.list_keys(None, 100)
```

#### `verify(entry)` - Verify Entry

Verifies hash integrity of single entry.

```python
is_valid = client.verify(entry)
```

#### `verify_log_chain(limit)` - Verify Chain

Verifies hash chain integrity.

```python
is_valid = client.verify_log_chain(100)
```

## Guarantees and Limitations

### What This System Guarantees

- ✅ No concurrent write conflicts (optimistic locking with retry)
- ✅ Log is always updated first (data never lost)
- ✅ Log is immutable and append-only
- ✅ Hash chain integrity (tampering breaks subsequent hashes)
- ✅ Global ordering (log versions provide chronological order)
- ✅ Eventual consistency (orphans repaired automatically)
- ✅ Bounded repair time (within `repair_check_interval_ms` of activity)

### What This System Does NOT Guarantee

- ❌ Immediate consistency (key object write can fail temporarily)
- ❌ Transactional semantics (not ACID - log + key are separate writes)
- ❌ Latest entry always consistent (most recent may be orphaned briefly)

## Use Cases

### Good Fit

- Audit logs needing global ordering
- Configuration management with history
- Calibration parameters for IoT devices
- Lambda/serverless environments
- Simple compliance logging
- Applications tolerating eventual consistency

### Not a Good Fit

- Need guaranteed immediate consistency
- Need sub-second repair guarantees
- High-frequency writes (>100/sec per key)
- Applications requiring ACID transactions

## Cost Analysis

Based on 1M write operations:

| Component | Cost |
|-----------|------|
| S3 PUT requests (log) | 1M × $0.005/1K = $5.00 |
| S3 PUT requests (keys) | 1M × $0.005/1K = $5.00 |
| S3 GET requests | 1M × $0.0004/1K = $0.40 |
| S3 storage (1KB/entry) | 1GB × $0.023 = $0.023 |
| **Total** | **~$10.42** |

DynamoDB equivalent: ~$1.25/M writes + ~$5/M reads = **$6.25+** (plus storage)

ImmuKV is cost-effective for audit log patterns with occasional reads.

## Development

### Repository Structure

```
immukv/
├── python/              # Python package
│   ├── src/immukv/
│   │   ├── __init__.py
│   │   ├── client.py   # Main client implementation
│   │   └── types.py    # Type definitions
│   ├── tests/
│   └── pyproject.toml
├── typescript/          # TypeScript package
│   ├── src/
│   │   ├── index.ts
│   │   ├── client.ts   # Main client implementation
│   │   └── types.ts    # Type definitions
│   ├── tests/
│   └── package.json
└── README.md
```

### Testing

```bash
# Python
cd python
pip install -e ".[dev]"
pytest

# TypeScript
cd typescript
npm install
npm test
```

## Design Documentation

For detailed design specifications, see the design document at:
`../d85065f19dcc645bd1fd14cb2beb92be/immukv-simple-versioning-design.md`

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or pull request.
