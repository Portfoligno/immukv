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

## API

### `ImmuKVProps`

Top-level properties for the `ImmuKV` construct:

- `bucketName` (optional): Name for the S3 bucket. If not specified, an auto-generated bucket name will be used.
- `useKmsEncryption` (optional): Enable KMS encryption instead of S3-managed encryption (default: false).
- `prefixes` (required): Array of `ImmuKVPrefixConfig` entries. At least one entry is required.

### `ImmuKVPrefixConfig`

Configuration for a single ImmuKV prefix within the bucket:

- `s3Prefix` (required): S3 key prefix for this namespace. Use `""` for bucket root, or directory-style like `"myapp/"` for namespacing.
- `logVersionRetention` (optional): Duration to retain old log versions. Must be expressible in whole days.
- `logVersionsToRetain` (optional): Number of old log versions to retain.
- `keyVersionRetention` (optional): Duration to retain old key object versions. Must be expressible in whole days.
- `keyVersionsToRetain` (optional): Number of old key versions to retain per key.
- `onLogEntryCreated` (optional): S3 notification destination triggered when log entries are created under this prefix. Supports Lambda, SNS, and SQS.
- `oidcProviders` (optional): Array of OIDC identity providers for web identity federation scoped to this prefix. Each provider has an `issuerUrl` (must start with `"https://"`) and `clientIds` (audiences to trust).
- `oidcReadOnly` (optional): Whether the federated role gets read-only access instead of read-write (default: false).

### Prefix Validation Rules

- Prefixes must not start with `/` or contain `..`
- Duplicate prefixes are not allowed
- Overlapping prefixes are not allowed (one being a prefix of the other)
- Empty string prefix `""` cannot coexist with other prefixes (it matches all objects)

### `ImmuKV` Class

The `ImmuKV` construct exposes:

- `bucket`: The S3 bucket shared by all prefixes.
- `prefixes`: Object mapping prefix strings to `ImmuKVPrefixResources`.
- `prefix(s3Prefix)`: Method to get resources for a specific prefix (throws if not found).

### `ImmuKVPrefixResources`

Resources created for each prefix:

- `s3Prefix`: The S3 prefix string (as provided in the config).
- `readWritePolicy`: IAM managed policy granting read-write access scoped to this prefix.
- `readOnlyPolicy`: IAM managed policy granting read-only access scoped to this prefix.
- `federatedRole` (optional): Federated IAM role for OIDC users scoped to this prefix. Only present when `oidcProviders` was specified.

## License

MIT
