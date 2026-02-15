import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

/**
 * Standard file names and patterns used by ImmuKV
 */
const LOG_FILE_PATTERN = "_log.json";
const KEYS_PREFIX = "keys/";

export interface OidcProvider {
  /** OIDC issuer URL (e.g., 'https://accounts.google.com') */
  readonly issuerUrl: string;
  /** Client IDs (audiences) to trust from this provider */
  readonly clientIds: string[];
}

export interface ImmuKVProps {
  /**
   * Name of the S3 bucket for ImmuKV storage
   * @default - Auto-generated bucket name
   */
  readonly bucketName?: string;

  /**
   * S3 prefix for all ImmuKV objects
   *
   * Controls where ImmuKV stores its data within the S3 bucket:
   * - Empty string or undefined: Files stored at bucket root (e.g., `_log.json`, `keys/mykey.json`)
   * - Without trailing slash (e.g., `myapp`): Flat prefix (e.g., `myapp_log.json`, `myappkeys/mykey.json`)
   * - With trailing slash (e.g., `myapp/`): Directory-style prefix (e.g., `myapp/_log.json`, `myapp/keys/mykey.json`)
   *
   * Note: S3 event notifications use prefix matching, so the filter will match any object
   * starting with `${s3Prefix}_log.json` (e.g., `_log.json`, `_log.json.backup`, etc.).
   * This is intentional behavior.
   *
   * @default - No prefix (root of bucket)
   */
  readonly s3Prefix?: string;

  /**
   * Duration to retain old log versions
   *
   * If specified, old log versions will be deleted after this duration.
   * Must be expressible in whole days (e.g., Duration.days(365)).
   * Can be used independently or combined with logVersionsToRetain.
   *
   * @default undefined - No time-based deletion (keep forever)
   */
  readonly logVersionRetention?: cdk.Duration;

  /**
   * Number of old log versions to retain
   *
   * If specified, only this many old log versions will be kept.
   * Can be used independently or combined with logVersionRetention.
   *
   * @default undefined - No count-based deletion (keep all versions)
   */
  readonly logVersionsToRetain?: number;

  /**
   * Duration to retain old key object versions
   *
   * If specified, old key versions will be deleted after this duration.
   * Must be expressible in whole days (e.g., Duration.days(180)).
   * Can be used independently or combined with keyVersionsToRetain.
   *
   * @default undefined - No time-based deletion (keep forever)
   */
  readonly keyVersionRetention?: cdk.Duration;

  /**
   * Number of old key versions to retain per key
   *
   * If specified, only this many old versions will be kept per key.
   * Can be used independently or combined with keyVersionRetention.
   *
   * @default undefined - No count-based deletion (keep all versions)
   */
  readonly keyVersionsToRetain?: number;

  /**
   * Enable KMS encryption instead of S3-managed encryption
   * @default false
   */
  readonly useKmsEncryption?: boolean;

  /**
   * Optional notification destination to trigger when log entries are created.
   * Supports Lambda functions, SNS topics, and SQS queues.
   *
   * Example with Lambda:
   * ```ts
   * import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
   *
   * new ImmuKV(this, 'ImmuKV', {
   *   onLogEntryCreated: new s3n.LambdaDestination(myFunction)
   * });
   * ```
   *
   * Example with SNS:
   * ```ts
   * new ImmuKV(this, 'ImmuKV', {
   *   onLogEntryCreated: new s3n.SnsDestination(myTopic)
   * });
   * ```
   *
   * Example with SQS:
   * ```ts
   * new ImmuKV(this, 'ImmuKV', {
   *   onLogEntryCreated: new s3n.SqsDestination(myQueue)
   * });
   * ```
   *
   * @default - No event notification configured
   */
  readonly onLogEntryCreated?: s3.IBucketNotificationDestination;

  /**
   * OIDC identity providers for web identity federation
   *
   * When specified, creates IAM OIDC providers and a federated IAM role
   * that allows users authenticated by these providers to assume temporary
   * AWS credentials via STS AssumeRoleWithWebIdentity.
   *
   * The federated role receives the readWritePolicy by default.
   * Set `oidcReadOnly: true` to use readOnlyPolicy instead.
   *
   * Example with Google:
   * ```ts
   * new ImmuKV(this, 'ImmuKV', {
   *   oidcProviders: [{
   *     issuerUrl: 'https://accounts.google.com',
   *     clientIds: ['123456789-abcdef.apps.googleusercontent.com'],
   *   }],
   * });
   * ```
   *
   * Example with multiple providers:
   * ```ts
   * new ImmuKV(this, 'ImmuKV', {
   *   oidcProviders: [{
   *     issuerUrl: 'https://accounts.google.com',
   *     clientIds: ['google-client-id'],
   *   }, {
   *     issuerUrl: 'https://login.microsoftonline.com/tenant-id/v2.0',
   *     clientIds: ['azure-app-id'],
   *   }],
   *   oidcReadOnly: true,  // federated users get read-only access
   * });
   * ```
   *
   * @default - No OIDC federation configured
   */
  readonly oidcProviders?: OidcProvider[];

  /**
   * Whether federated users get read-only access instead of read-write
   * @default false
   */
  readonly oidcReadOnly?: boolean;
}

/**
 * AWS CDK Construct for ImmuKV infrastructure
 *
 * Creates an S3 bucket with versioning enabled and IAM policies for
 * read/write and read-only access.
 */
export class ImmuKV extends Construct {
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

  /**
   * IAM role for OIDC-federated users (if oidcProviders was specified)
   */
  public readonly federatedRole?: iam.Role;

  constructor(scope: Construct, id: string, props?: ImmuKVProps) {
    super(scope, id);

    const logVersionRetention = props?.logVersionRetention;
    const logVersionsToRetain = props?.logVersionsToRetain;
    const keyVersionRetention = props?.keyVersionRetention;
    const keyVersionsToRetain = props?.keyVersionsToRetain;
    const s3Prefix = props?.s3Prefix ?? "";

    // Validate retention parameters if provided
    if (logVersionRetention !== undefined) {
      const days = logVersionRetention.toDays();
      if (!Number.isInteger(days) || days <= 0) {
        throw new Error(
          "logVersionRetention must be expressible as a positive whole number of days",
        );
      }
    }
    if (
      logVersionsToRetain !== undefined &&
      (logVersionsToRetain < 0 || !Number.isInteger(logVersionsToRetain))
    ) {
      throw new Error("logVersionsToRetain must be a non-negative integer");
    }
    if (keyVersionRetention !== undefined) {
      const days = keyVersionRetention.toDays();
      if (!Number.isInteger(days) || days <= 0) {
        throw new Error(
          "keyVersionRetention must be expressible as a positive whole number of days",
        );
      }
    }
    if (
      keyVersionsToRetain !== undefined &&
      (keyVersionsToRetain < 0 || !Number.isInteger(keyVersionsToRetain))
    ) {
      throw new Error("keyVersionsToRetain must be a non-negative integer");
    }

    // Validate s3Prefix
    if (s3Prefix && (s3Prefix.startsWith("/") || s3Prefix.includes(".."))) {
      throw new Error('s3Prefix must not start with "/" or contain ".."');
    }

    // Build lifecycle rules array conditionally
    const lifecycleRules: s3.LifecycleRule[] = [];

    // Add log lifecycle rule if any retention parameter is specified
    if (
      logVersionRetention !== undefined ||
      logVersionsToRetain !== undefined
    ) {
      lifecycleRules.push({
        id: "delete-old-log-versions",
        enabled: true,
        noncurrentVersionExpiration: logVersionRetention,
        noncurrentVersionsToRetain: logVersionsToRetain,
        prefix: `${s3Prefix}${LOG_FILE_PATTERN}`,
      });
    }

    // Add key lifecycle rule if any retention parameter is specified
    if (
      keyVersionRetention !== undefined ||
      keyVersionsToRetain !== undefined
    ) {
      lifecycleRules.push({
        id: "delete-old-key-versions",
        enabled: true,
        noncurrentVersionExpiration: keyVersionRetention,
        noncurrentVersionsToRetain: keyVersionsToRetain,
        prefix: `${s3Prefix}${KEYS_PREFIX}`,
      });
    }

    // S3 Bucket with versioning
    this.bucket = new s3.Bucket(this, "ImmuKVBucket", {
      bucketName: props?.bucketName,
      versioned: true,
      encryption: props?.useKmsEncryption
        ? s3.BucketEncryption.KMS_MANAGED
        : s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: lifecycleRules.length > 0 ? lifecycleRules : undefined,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // IAM Policy for read/write access (Lambda, EC2, ECS, etc.)
    this.readWritePolicy = new iam.ManagedPolicy(
      this,
      "ImmuKVReadWritePolicy",
      {
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
              "s3:GetObject",
              "s3:GetObjectVersion",
              "s3:PutObject",
              "s3:ListBucket",
              "s3:ListBucketVersions",
              "s3:HeadObject",
            ],
            resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
          }),
        ],
      },
    );

    // IAM Policy for read-only devices (sensors, IoT devices, etc.)
    this.readOnlyPolicy = new iam.ManagedPolicy(this, "ImmuKVReadOnlyPolicy", {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:ListBucketVersions",
            "s3:HeadObject",
          ],
          resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
        }),
      ],
    });

    // S3 Event Notification (optional)
    if (props?.onLogEntryCreated) {
      this.bucket.addEventNotification(
        s3.EventType.OBJECT_CREATED,
        props.onLogEntryCreated,
        { prefix: `${s3Prefix}${LOG_FILE_PATTERN}` },
      );
    }

    // OIDC Federation (optional)
    if (props?.oidcProviders !== undefined && props.oidcProviders.length > 0) {
      for (const [i, provider] of props.oidcProviders.entries()) {
        if (!provider.issuerUrl.startsWith("https://")) {
          throw new Error(
            `oidcProviders[${i}].issuerUrl must start with "https://", got: ${provider.issuerUrl}`,
          );
        }
        if (provider.clientIds.length === 0) {
          throw new Error(
            `oidcProviders[${i}].clientIds must contain at least one element`,
          );
        }
      }

      const oidcProviders = props.oidcProviders.map(
        (provider, i) =>
          new iam.OpenIdConnectProvider(this, `OidcProvider${i}`, {
            url: provider.issuerUrl,
            clientIds: provider.clientIds,
          }),
      );

      this.federatedRole = new iam.Role(this, "ImmuKVFederatedRole", {
        assumedBy: new iam.CompositePrincipal(
          ...oidcProviders.map(
            (provider, i) =>
              new iam.WebIdentityPrincipal(provider.openIdConnectProviderArn, {
                StringEquals: {
                  [`${props.oidcProviders![i]!.issuerUrl.replace(/^https?:\/\//, "")}:aud`]:
                    props.oidcProviders![i]!.clientIds,
                },
              }),
          ),
        ),
        maxSessionDuration: cdk.Duration.hours(1),
      });

      this.federatedRole.addManagedPolicy(
        props.oidcReadOnly === true
          ? this.readOnlyPolicy
          : this.readWritePolicy,
      );
    }
  }
}
