# ImmuKV - Python Client

Lightweight immutable key-value store using S3 versioning.

## Installation

```bash
pip install immukv
```

## Quick Start

```python
from immukv import ImmuKVClient, Config

config = Config(
    s3_bucket="your-bucket",
    s3_region="us-east-1",
    s3_prefix=""
)

with ImmuKVClient(config) as client:
    # Write
    entry = client.set("key1", {"value": "data"})
    print(f"Committed: {entry.version_id}")

    # Read
    latest = client.get("key1")
    print(f"Latest: {latest.value}")
```

## Features

- **Immutable log** - All writes append to global log
- **Fast reads** - Single S3 request for latest value
- **Hash chain** - Cryptographic integrity verification
- **No database** - Uses S3 versioning only
- **Auto-repair** - Orphaned entries repaired automatically

See the [full documentation](../../README.md) for more details.
