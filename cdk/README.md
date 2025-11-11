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

### TypeScript

```typescript
import * as cdk from 'aws-cdk-lib';
import { ImmuKVStack } from 'cdk-immukv';

const app = new cdk.App();
new ImmuKVStack(app, 'MyImmuKVStack', {
  bucketName: 'my-immukv-bucket',
  s3Prefix: 'myapp/',
});
```

### Python

```python
import aws_cdk as cdk
from cdk_immukv import ImmuKVStack

app = cdk.App()
ImmuKVStack(app, "MyImmuKVStack",
    bucket_name="my-immukv-bucket",
    s3_prefix="myapp/",
)
```

## API

See the [API documentation](https://github.com/Portfoligno/immukv/tree/main/cdk) for detailed information.

## License

MIT

