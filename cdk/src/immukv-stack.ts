import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface ImmuKVStackProps extends cdk.StackProps {
  /**
   * Name of the S3 bucket for ImmuKV storage
   * @default - Auto-generated bucket name
   */
  readonly bucketName?: string;

  /**
   * S3 prefix for all ImmuKV objects
   * @default - No prefix (root of bucket)
   */
  readonly s3Prefix?: string;

  /**
   * Number of days to retain old log versions
   * @default 365
   */
  readonly logVersionRetentionDays?: number;

  /**
   * Number of old log versions to retain
   * @default 1000
   */
  readonly logVersionsToRetain?: number;

  /**
   * Number of days to retain old key object versions
   * @default 365
   */
  readonly keyVersionRetentionDays?: number;

  /**
   * Number of old key versions to retain per key
   * @default 100
   */
  readonly keyVersionsToRetain?: number;

  /**
   * Enable KMS encryption instead of S3-managed encryption
   * @default false
   */
  readonly useKmsEncryption?: boolean;
}

/**
 * AWS CDK Stack for ImmuKV infrastructure
 *
 * Creates an S3 bucket with versioning enabled and IAM policies for
 * read/write and read-only access.
 */
export class ImmuKVStack extends cdk.Stack {
  /**
   * The S3 bucket used for ImmuKV storage
   */
  public readonly bucket: s3.Bucket;

  /**
   * IAM managed policy for read/write access
   */
  public readonly readWritePolicy: iam.ManagedPolicy;

  /**
   * IAM managed policy for read-only access
   */
  public readonly readOnlyPolicy: iam.ManagedPolicy;

  constructor(scope: Construct, id: string, props?: ImmuKVStackProps) {
    super(scope, id, props);

    const logVersionRetentionDays = props?.logVersionRetentionDays ?? 365;
    const logVersionsToRetain = props?.logVersionsToRetain ?? 1000;
    const keyVersionRetentionDays = props?.keyVersionRetentionDays ?? 365;
    const keyVersionsToRetain = props?.keyVersionsToRetain ?? 100;
    const s3Prefix = props?.s3Prefix ?? '';

    // S3 Bucket with versioning
    this.bucket = new s3.Bucket(this, 'ImmuKVBucket', {
      bucketName: props?.bucketName,
      versioned: true,
      encryption: props?.useKmsEncryption
        ? s3.BucketEncryption.KMS_MANAGED
        : s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'delete-old-log-versions',
          enabled: true,
          noncurrentVersionExpiration: cdk.Duration.days(logVersionRetentionDays),
          noncurrentVersionsToRetain: logVersionsToRetain,
          prefix: `${s3Prefix}_log.json`,
        },
        {
          id: 'delete-old-key-versions',
          enabled: true,
          noncurrentVersionExpiration: cdk.Duration.days(keyVersionRetentionDays),
          noncurrentVersionsToRetain: keyVersionsToRetain,
          prefix: `${s3Prefix}keys/`,
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // IAM Policy for read/write access (Lambda, EC2, ECS, etc.)
    this.readWritePolicy = new iam.ManagedPolicy(this, 'ImmuKVReadWritePolicy', {
      managedPolicyName: `immukv-readwrite-${this.stackName}`,
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:GetObject',
            's3:GetObjectVersion',
            's3:PutObject',
            's3:ListBucket',
            's3:ListBucketVersions',
            's3:HeadObject',
          ],
          resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
        }),
      ],
    });

    // IAM Policy for read-only devices (sensors, IoT devices, etc.)
    this.readOnlyPolicy = new iam.ManagedPolicy(this, 'ImmuKVReadOnlyPolicy', {
      managedPolicyName: `immukv-readonly-${this.stackName}`,
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:GetObject',
            's3:GetObjectVersion',
            's3:ListBucket',
            's3:ListBucketVersions',
            's3:HeadObject',
          ],
          resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
        }),
      ],
    });

    // Outputs
    new cdk.CfnOutput(this, 'BucketName', {
      value: this.bucket.bucketName,
      description: 'ImmuKV S3 Bucket Name',
    });

    new cdk.CfnOutput(this, 'BucketArn', {
      value: this.bucket.bucketArn,
      description: 'ImmuKV S3 Bucket ARN',
    });

    new cdk.CfnOutput(this, 'ReadWritePolicyArn', {
      value: this.readWritePolicy.managedPolicyArn,
      description: 'IAM Policy ARN for read/write access',
    });

    new cdk.CfnOutput(this, 'ReadOnlyPolicyArn', {
      value: this.readOnlyPolicy.managedPolicyArn,
      description: 'IAM Policy ARN for read-only access',
    });
  }
}

