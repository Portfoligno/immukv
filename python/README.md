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

# Identity functions for JSON values (use custom encoders/decoders for complex types)
def identity(x): return x

with ImmuKVClient(config, identity, identity) as client:
    # Write
    entry = client.set("key1", {"value": "data"})
    print(f"Committed: {entry.version_id}")

    # Read
    latest = client.get("key1")
    print(f"Latest: {latest.value}")

    # List keys
    keys = client.list_keys(None, 100)

    # List keys with prefix filtering (server-side)
    sensor_keys = client.list_keys_with_prefix("sensor-", None, 100)
```

## Features

- **Immutable log** - All writes append to global log
- **Fast reads** - Single S3 request for latest value
- **Hash chain** - Cryptographic integrity verification
- **No database** - Uses S3 versioning only
- **Auto-repair** - Orphaned entries repaired automatically
- **Credential providers** - Pluggable async credential refresh via `CredentialProvider`
- **Sync API** - Synchronous interface backed by aiobotocore with a background event loop

## Credential Providers

The client supports static credentials or an async credential provider for dynamic credential refresh (e.g., OIDC federation). The credential provider is backed by `AioDeferredRefreshableCredentials` for automatic refresh.

```python
from immukv import ImmuKVClient, Config, S3Overrides, S3Credentials

# Static credentials
config = Config(
    s3_bucket="bucket",
    s3_region="us-east-1",
    s3_prefix="",
    overrides=S3Overrides(
        credentials=S3Credentials(
            aws_access_key_id="AKIA...",
            aws_secret_access_key="...",
            aws_session_token="...",
        ),
    ),
)

# Async credential provider
async def my_provider() -> S3Credentials:
    return S3Credentials(
        aws_access_key_id="AKIA...",
        aws_secret_access_key="...",
        aws_session_token="...",       # Optional
        expires_at=some_datetime,      # Optional (defaults to 1 hour from now)
    )

config_with_provider = Config(
    s3_bucket="bucket",
    s3_region="us-east-1",
    s3_prefix="",
    overrides=S3Overrides(credentials=my_provider),
)
```

See the [full documentation](../README.md) for more details.
