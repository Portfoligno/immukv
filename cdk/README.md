# cdk-immukv

AWS CDK constructs for deploying ImmuKV infrastructure.

## Installation

### TypeScript/JavaScript

```bash
npm install cdk-immukv
```

### Python

```bash
pip install cdk-immukv
```

## Usage

### Basic Setup

The `ImmuKV` construct uses a multi-prefix architecture. Each prefix defines an isolated ImmuKV namespace within a shared S3 bucket, with its own lifecycle rules, event notifications, IAM policies, and optional OIDC federation.

#### TypeScript

```typescript
import * as cdk from "aws-cdk-lib";
import { ImmuKV } from "cdk-immukv";

const app = new cdk.App();
const stack = new cdk.Stack(app, "MyStack");

// Single prefix at bucket root
const store = new ImmuKV(stack, "ImmuKV", {
  bucketName: "my-immukv-bucket",
  prefixes: [{ s3Prefix: "" }],
});

// Access the prefix's IAM policies
store.prefix("").readWritePolicy;
store.prefix("").readOnlyPolicy;
```

#### Python

```python
import aws_cdk as cdk
from cdk_immukv import ImmuKV

app = cdk.App()
stack = cdk.Stack(app, "MyStack")

store = ImmuKV(stack, "ImmuKV",
    bucket_name="my-immukv-bucket",
    prefixes=[{"s3_prefix": ""}],
)
```

### Multi-Prefix Setup

Multiple prefixes share a single S3 bucket while remaining fully isolated at the IAM level.

```typescript
import * as cdk from "aws-cdk-lib";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { ImmuKV } from "cdk-immukv";

const store = new ImmuKV(stack, "ImmuKV", {
  prefixes: [
    {
      s3Prefix: "pipeline/",
      logVersionRetention: cdk.Duration.days(2555),
      onLogEntryCreated: new s3n.LambdaDestination(shadowUpdateFn),
    },
    {
      s3Prefix: "config/",
      logVersionRetention: cdk.Duration.days(90),
      onLogEntryCreated: new s3n.LambdaDestination(configSyncFn),
    },
  ],
});

// Access per-prefix resources
store.prefix("pipeline/").readWritePolicy;
store.prefix("config/").readOnlyPolicy;
```

### S3 Event Notifications

Event notifications are configured per-prefix. Each prefix can have its own notification destination triggered when log entries are created. Supports Lambda functions, SNS topics, and SQS queues.

#### TypeScript - Lambda Trigger

```typescript
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { ImmuKV } from "cdk-immukv";

const processorFn = new lambda.Function(stack, "LogProcessor", {
  runtime: lambda.Runtime.PYTHON_3_11,
  handler: "index.handler",
  code: lambda.Code.fromAsset("lambda"),
});

new ImmuKV(stack, "ImmuKV", {
  bucketName: "my-immukv-bucket",
  prefixes: [
    {
      s3Prefix: "",
      onLogEntryCreated: new s3n.LambdaDestination(processorFn),
    },
  ],
});
```

#### TypeScript - SNS Topic

```typescript
import * as sns from "aws-cdk-lib/aws-sns";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { ImmuKV } from "cdk-immukv";

const topic = new sns.Topic(stack, "LogEntryTopic");

new ImmuKV(stack, "ImmuKV", {
  bucketName: "my-immukv-bucket",
  prefixes: [
    {
      s3Prefix: "",
      onLogEntryCreated: new s3n.SnsDestination(topic),
    },
  ],
});
```

#### TypeScript - SQS Queue

```typescript
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { ImmuKV } from "cdk-immukv";

const queue = new sqs.Queue(stack, "LogEntryQueue");

new ImmuKV(stack, "ImmuKV", {
  bucketName: "my-immukv-bucket",
  prefixes: [
    {
      s3Prefix: "",
      onLogEntryCreated: new s3n.SqsDestination(queue),
    },
  ],
});
```

#### Python - Lambda Trigger

```python
import aws_cdk as cdk
from aws_cdk import aws_lambda as lambda_
from aws_cdk.aws_s3_notifications import LambdaDestination
from cdk_immukv import ImmuKV

processor_fn = lambda_.Function(stack, "LogProcessor",
    runtime=lambda_.Runtime.PYTHON_3_11,
    handler="index.handler",
    code=lambda_.Code.from_asset("lambda"),
)

ImmuKV(stack, "ImmuKV",
    bucket_name="my-immukv-bucket",
    prefixes=[{
        "s3_prefix": "",
        "on_log_entry_created": LambdaDestination(processor_fn),
    }],
)
```

### OIDC Federation

OIDC identity providers are configured per-prefix. Each prefix can have its own federated IAM role scoped to that prefix's resources.

```typescript
import { ImmuKV } from "cdk-immukv";

const store = new ImmuKV(stack, "ImmuKV", {
  prefixes: [
    {
      s3Prefix: "app/",
      oidcProviders: [
        {
          issuerUrl: "https://accounts.google.com",
          clientIds: ["your-client-id.apps.googleusercontent.com"],
        },
      ],
      // oidcReadOnly: true,  // Set to true for read-only federated access
    },
  ],
});

// The federated role is available on the prefix resources
store.prefix("app/").federatedRole; // IAM role for OIDC users
```

#### Email-Based Access Control

Use `allowedEmails` on an `OidcProvider` to restrict federation to specific email addresses. This adds a `StringEquals` condition on `${issuerUrl}:email` to the trust policy.

```typescript
const store = new ImmuKV(stack, "ImmuKV", {
  prefixes: [
    {
      s3Prefix: "app/",
      oidcProviders: [
        {
          issuerUrl: "https://accounts.google.com",
          clientIds: ["your-client-id.apps.googleusercontent.com"],
          allowedEmails: ["alice@example.com", "bob@example.com"],
        },
      ],
    },
  ],
});
```

When `allowedEmails` is omitted, the trust policy only checks `:aud` (client ID). When provided, it must be non-empty.

#### Provider Deduplication

OIDC providers are deduplicated by issuer URL. If the same issuer URL appears across multiple prefixes (literal or wildcard), a single IAM OIDC provider resource is created and shared. Issuer URLs are normalized (`new URL(issuerUrl).href`) so that cosmetic variants like trailing slashes, mixed-case hostnames, and default ports are treated as the same provider.

If the same issuer URL is referenced with different `clientIds` across prefixes, the construct throws an error at synthesis time.

### Wildcard Prefixes

Wildcard prefixes provide IAM-only scoping using patterns like `tenant-*/logs/*`. They support `*` (zero or more characters) and `?` (exactly one character) via IAM `StringLike` conditions and ARN matching.

Wildcard prefixes do **not** support S3 lifecycle rules, event notifications, or listing -- those require literal prefixes. They are purely for IAM policy scoping.

```typescript
import { ImmuKV } from "cdk-immukv";

const store = new ImmuKV(stack, "ImmuKV", {
  prefixes: [{ s3Prefix: "" }],
  wildcardPrefixes: [
    {
      pattern: "tenant-*/logs/",
      oidcProviders: [
        {
          issuerUrl: "https://accounts.google.com",
          clientIds: ["your-client-id.apps.googleusercontent.com"],
        },
      ],
    },
    {
      pattern: "*/config/",
      oidcReadOnly: true,
      oidcProviders: [
        {
          issuerUrl: "https://accounts.google.com",
          clientIds: ["your-client-id.apps.googleusercontent.com"],
        },
      ],
    },
  ],
});

// Access wildcard prefix resources
store.wildcardPrefix("tenant-*/logs/").readWritePolicy;
store.wildcardPrefix("tenant-*/logs/").federatedRole;
```

## API

### `OidcProvider`

Configuration for an OIDC identity provider:

- `issuerUrl` (required): OIDC issuer URL (must start with `"https://"`).
- `clientIds` (required): Client IDs (audiences) to trust from this provider. Must contain at least one element.
- `allowedEmails` (optional): Email addresses allowed to assume the federated role. Adds a `StringEquals` condition on `${issuerUrl}:email` to the trust policy. Must be non-empty when provided. When omitted, the trust policy only checks `:aud` (client ID).

### `ImmuKVProps`

Top-level properties for the `ImmuKV` construct:

- `bucketName` (optional): Name for the S3 bucket. If not specified, an auto-generated bucket name will be used.
- `useKmsEncryption` (optional): Enable KMS encryption instead of S3-managed encryption (default: false).
- `prefixes` (required): Array of `ImmuKVPrefixConfig` entries. At least one entry is required.
- `wildcardPrefixes` (optional): Array of `ImmuKVWildcardPrefixConfig` entries for IAM-only wildcard scoping.

### `ImmuKVPrefixConfig`

Configuration for a single ImmuKV prefix within the bucket:

- `s3Prefix` (required): S3 key prefix for this namespace. Use `""` for bucket root, or directory-style like `"myapp/"` for namespacing.
- `logVersionRetention` (optional): Duration to retain old log versions. Must be expressible in whole days.
- `logVersionsToRetain` (optional): Number of old log versions to retain.
- `keyVersionRetention` (optional): Duration to retain old key object versions. Must be expressible in whole days.
- `keyVersionsToRetain` (optional): Number of old key versions to retain per key.
- `onLogEntryCreated` (optional): S3 notification destination triggered when log entries are created under this prefix. Supports Lambda, SNS, and SQS.
- `oidcProviders` (optional): Array of `OidcProvider` entries for web identity federation scoped to this prefix.
- `oidcReadOnly` (optional): Whether the federated role gets read-only access instead of read-write (default: false).

### Prefix Validation Rules

- Prefixes must not start with `/` or contain `..`
- Duplicate prefixes are not allowed
- Overlapping prefixes are not allowed (one being a prefix of the other)
- Empty string prefix `""` cannot coexist with other prefixes (it matches all objects)

### `ImmuKVWildcardPrefixConfig`

Configuration for a wildcard-based IAM prefix pattern:

- `pattern` (required): Wildcard prefix pattern for IAM scoping. Supports `*` (zero or more characters) and `?` (exactly one character). Must not be empty, start with `/`, or contain `..`.
- `oidcProviders` (optional): Array of `OidcProvider` entries for web identity federation scoped to this wildcard pattern.
- `oidcReadOnly` (optional): Whether the federated role gets read-only access instead of read-write (default: false).

### `ImmuKV` Class

The `ImmuKV` construct exposes:

- `bucket`: The S3 bucket shared by all prefixes.
- `prefixes`: Object mapping prefix strings to `ImmuKVPrefixResources`.
- `wildcardPrefixes`: Object mapping wildcard pattern strings to `ImmuKVWildcardPrefixResources`.
- `prefix(s3Prefix)`: Method to get resources for a specific prefix (throws if not found).
- `wildcardPrefix(pattern)`: Method to get resources for a specific wildcard prefix pattern (throws if not found).

### `ImmuKVPrefixResources`

Resources created for each prefix:

- `s3Prefix`: The S3 prefix string (as provided in the config).
- `readWritePolicy`: IAM managed policy granting read-write access scoped to this prefix.
- `readOnlyPolicy`: IAM managed policy granting read-only access scoped to this prefix.
- `federatedRole` (optional): Federated IAM role for OIDC users scoped to this prefix. Only present when `oidcProviders` was specified.

### `ImmuKVWildcardPrefixResources`

Resources created for each wildcard prefix pattern:

- `pattern`: The wildcard pattern string (as provided in the config).
- `readWritePolicy`: IAM managed policy granting read-write access scoped to this wildcard pattern. Uses `StringLike` conditions on `s3:prefix`.
- `readOnlyPolicy`: IAM managed policy granting read-only access scoped to this wildcard pattern.
- `federatedRole` (optional): Federated IAM role for OIDC users scoped to this wildcard pattern. Only present when `oidcProviders` was specified.

## License

MIT
