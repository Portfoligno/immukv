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

#### TypeScript

```typescript
import * as cdk from 'aws-cdk-lib';
import { ImmuKV } from 'cdk-immukv';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'MyStack');

new ImmuKV(stack, 'ImmuKV', {
  bucketName: 'my-immukv-bucket',
  s3Prefix: 'myapp/',
});
```

#### Python

```python
import aws_cdk as cdk
from cdk_immukv import ImmuKV

app = cdk.App()
stack = cdk.Stack(app, "MyStack")

ImmuKV(stack, "ImmuKV",
    bucket_name="my-immukv-bucket",
    s3_prefix="myapp/",
)
```

### S3 Event Notifications

You can optionally configure S3 event notifications to trigger when log entries are created. This supports Lambda functions, SNS topics, and SQS queues.

All notification destinations can be configured using the `onLogEntryCreated` property. Destinations can be in the same stack or different stacks - the Construct pattern handles this cleanly.

#### TypeScript - Lambda Trigger

```typescript
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { ImmuKV } from 'cdk-immukv';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'MyStack');

// Create a Lambda function
const processorFn = new lambda.Function(stack, 'LogProcessor', {
  runtime: lambda.Runtime.PYTHON_3_11,
  handler: 'index.handler',
  code: lambda.Code.fromAsset('lambda'),
});

// Configure ImmuKV to trigger the Lambda on log entry creation
new ImmuKV(stack, 'ImmuKV', {
  bucketName: 'my-immukv-bucket',
  onLogEntryCreated: new s3n.LambdaDestination(processorFn),
});
```

#### TypeScript - SNS Topic

```typescript
import * as cdk from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { ImmuKV } from 'cdk-immukv';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'MyStack');

// Create SNS topic
const topic = new sns.Topic(stack, 'LogEntryTopic');

// Configure ImmuKV to publish to SNS on log entry creation
new ImmuKV(stack, 'ImmuKV', {
  bucketName: 'my-immukv-bucket',
  onLogEntryCreated: new s3n.SnsDestination(topic),
});
```

#### TypeScript - SQS Queue

```typescript
import * as cdk from 'aws-cdk-lib';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { ImmuKV } from 'cdk-immukv';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'MyStack');

// Create SQS queue
const queue = new sqs.Queue(stack, 'LogEntryQueue');

// Configure ImmuKV to send to SQS on log entry creation
new ImmuKV(stack, 'ImmuKV', {
  bucketName: 'my-immukv-bucket',
  onLogEntryCreated: new s3n.SqsDestination(queue),
});
```

#### Python - Lambda Trigger

```python
import aws_cdk as cdk
from aws_cdk import aws_lambda as lambda_
from aws_cdk.aws_s3_notifications import LambdaDestination
from cdk_immukv import ImmuKV

app = cdk.App()
stack = cdk.Stack(app, "MyStack")

# Create Lambda function
processor_fn = lambda_.Function(stack, "LogProcessor",
    runtime=lambda_.Runtime.PYTHON_3_11,
    handler="index.handler",
    code=lambda_.Code.from_asset("lambda"),
)

# Configure ImmuKV with Lambda trigger
ImmuKV(stack, "ImmuKV",
    bucket_name="my-immukv-bucket",
    on_log_entry_created=LambdaDestination(processor_fn),
)
```

## API

The `ImmuKV` construct accepts the following properties:
- `bucketName` (required): Name for the S3 bucket
- `s3Prefix` (optional): Prefix for S3 keys
- `onLogEntryCreated` (optional): S3 notification destination for log entry events

## License

MIT

