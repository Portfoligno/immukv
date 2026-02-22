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
  /** OIDC issuer URL (must start with "https://") */
  readonly issuerUrl: string;
  /** Client IDs (audiences) to trust from this provider */
  readonly clientIds: string[];
}

/**
 * Configuration for a single ImmuKV prefix within a bucket.
 *
 * Each prefix operates as an independent ImmuKV namespace with its own
 * lifecycle rules, event notifications, IAM policies, and optional OIDC
 * federation. Prefixes are isolated at the IAM level — a policy generated
 * for one prefix does not grant access to another.
 */
export interface ImmuKVPrefixConfig {
  /**
   * S3 key prefix for this ImmuKV namespace.
   *
   * Controls where ImmuKV stores its data within the S3 bucket:
   * - Empty string: Files at bucket root (`_log.json`, `keys/mykey.json`)
   * - With trailing slash (e.g., `myapp/`): Directory-style (`myapp/_log.json`, `myapp/keys/mykey.json`)
   * - Without trailing slash (e.g., `myapp`): Flat prefix (`myapp_log.json`, `myappkeys/mykey.json`)
   *
   * Validation:
   * - Must not start with `/` or contain `..`
   * - Must not duplicate another prefix in the same instance
   * - Must not overlap another prefix (one being a prefix of the other)
   * - Empty string `""` cannot coexist with other prefixes (it matches all objects)
   */
  readonly s3Prefix: string;

  /**
   * Duration to retain old log versions for this prefix.
   * Must be expressible in whole days.
   * @default undefined — keep forever
   */
  readonly logVersionRetention?: cdk.Duration;

  /**
   * Number of old log versions to retain for this prefix.
   * Must be a non-negative integer.
   * @default undefined — keep all versions
   */
  readonly logVersionsToRetain?: number;

  /**
   * Duration to retain old key object versions for this prefix.
   * Must be expressible in whole days.
   * @default undefined — keep forever
   */
  readonly keyVersionRetention?: cdk.Duration;

  /**
   * Number of old key versions to retain per key for this prefix.
   * Must be a non-negative integer.
   * @default undefined — keep all versions
   */
  readonly keyVersionsToRetain?: number;

  /**
   * Notification destination triggered when log entries are created
   * under this prefix. Supports Lambda, SNS, and SQS.
   *
   * Event filter matches `${s3Prefix}_log.json` via S3 prefix matching.
   * @default undefined — no notification
   */
  readonly onLogEntryCreated?: s3.IBucketNotificationDestination;

  /**
   * OIDC identity providers for web identity federation scoped to this prefix.
   *
   * Creates a federated IAM role whose policies are scoped to this prefix only.
   * The role receives this prefix's readWritePolicy by default,
   * or readOnlyPolicy if `oidcReadOnly` is true.
   *
   * @default undefined — no OIDC federation
   */
  readonly oidcProviders?: OidcProvider[];

  /**
   * Whether the federated role gets read-only access instead of read-write.
   * Only meaningful when `oidcProviders` is set.
   * @default false
   */
  readonly oidcReadOnly?: boolean;
}

/**
 * Resources created for a single ImmuKV prefix.
 */
export interface ImmuKVPrefixResources {
  /** The S3 prefix string (as provided in the config) */
  readonly s3Prefix: string;

  /**
   * IAM managed policy granting read-write access scoped to this prefix.
   *
   * Object actions (GetObject, GetObjectVersion, PutObject, HeadObject)
   * on `bucketArn/${prefix}*`. Bucket actions (ListBucket, ListBucketVersions)
   * on `bucketArn` with `s3:prefix` condition.
   */
  readonly readWritePolicy: iam.ManagedPolicy;

  /**
   * IAM managed policy granting read-only access scoped to this prefix.
   * Same as readWritePolicy but without PutObject.
   */
  readonly readOnlyPolicy: iam.ManagedPolicy;

  /**
   * Federated IAM role for OIDC users scoped to this prefix.
   * Only present when `oidcProviders` was specified.
   */
  readonly federatedRole?: iam.Role;
}

export interface ImmuKVProps {
  /**
   * Name of the S3 bucket.
   * @default — auto-generated
   */
  readonly bucketName?: string;

  /**
   * Enable KMS encryption instead of S3-managed encryption.
   * @default false
   */
  readonly useKmsEncryption?: boolean;

  /**
   * Prefix configurations. At least one entry is required.
   *
   * Each entry defines an isolated ImmuKV namespace within the shared bucket,
   * with its own lifecycle rules, event notifications, IAM policies,
   * and optional OIDC federation.
   *
   * @example
   *
   * // Single prefix (migration from old API):
   * new ImmuKV(this, 'Store', {
   *   prefixes: [{ s3Prefix: '', logVersionRetention: cdk.Duration.days(365) }],
   * });
   *
   * @example
   *
   * // Two prefixes with different retention and notifications:
   * new ImmuKV(this, 'Store', {
   *   prefixes: [
   *     { s3Prefix: 'pipeline/', logVersionRetention: cdk.Duration.days(2555),
   *       onLogEntryCreated: new s3n.LambdaDestination(shadowUpdateFn) },
   *     { s3Prefix: 'config/', logVersionRetention: cdk.Duration.days(90),
   *       onLogEntryCreated: new s3n.LambdaDestination(configSyncFn) },
   *   ],
   * });
   */
  readonly prefixes: ImmuKVPrefixConfig[];
}

/**
 * Converts an S3 prefix string to a CDK construct-safe ID suffix.
 */
function prefixToConstructId(s3Prefix: string): string {
  if (s3Prefix === "") return "Root";
  return s3Prefix
    .replace(/\/+$/, "")
    .replace(/[^a-zA-Z0-9]/g, " ")
    .trim()
    .split(/\s+/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join("");
}

/**
 * AWS CDK Construct for ImmuKV infrastructure
 *
 * Creates an S3 bucket with versioning enabled and per-prefix IAM policies,
 * lifecycle rules, event notifications, and optional OIDC federation.
 */
export class ImmuKV extends Construct {
  /** The S3 bucket shared by all prefixes. */
  public readonly bucket: s3.Bucket;

  /**
   * Per-prefix resources, keyed by the s3Prefix string.
   * Use `prefix()` for type-safe access with runtime validation.
   */
  public readonly prefixes: { [key: string]: ImmuKVPrefixResources };

  /**
   * Get resources for a specific prefix.
   * @throws Error if no prefix with that name exists.
   *
   * @example
   * instance.prefix("config/").readWritePolicy
   * instance.prefix("").readWritePolicy  // root prefix
   */
  public prefix(s3Prefix: string): ImmuKVPrefixResources {
    const resources = this.prefixes[s3Prefix];
    if (!resources) {
      throw new Error(
        `No prefix "${s3Prefix}" configured. Available: ${Object.keys(
          this.prefixes,
        )
          .map((k) => `"${k}"`)
          .join(", ")}`,
      );
    }
    return resources;
  }

  constructor(scope: Construct, id: string, props: ImmuKVProps) {
    super(scope, id);

    // ── Validation ──

    // At least one prefix
    if (props.prefixes.length === 0) {
      throw new Error("prefixes must contain at least one element");
    }

    // Duplicate check
    const seen = new Set<string>();
    for (const pc of props.prefixes) {
      if (seen.has(pc.s3Prefix)) {
        throw new Error(`Duplicate prefix: "${pc.s3Prefix}"`);
      }
      seen.add(pc.s3Prefix);
    }

    // Overlap check: empty string overlaps everything
    const prefixStrings = props.prefixes.map((p) => p.s3Prefix);
    if (prefixStrings.includes("") && prefixStrings.length > 1) {
      throw new Error(
        'Empty-string prefix ("") cannot coexist with other prefixes — ' +
          "it matches all objects. Use it alone, or use directory-style " +
          'prefixes (e.g., "pipeline/") for multi-prefix setups.',
      );
    }

    // Overlap check: sorted prefix containment
    const sorted = [...prefixStrings].sort();
    for (let i = 0; i < sorted.length - 1; i++) {
      if (sorted[i + 1]!.startsWith(sorted[i]!)) {
        throw new Error(
          `Overlapping prefixes: "${sorted[i]}" is a prefix of "${sorted[i + 1]}". ` +
            "S3 lifecycle rules and event notifications for the shorter prefix " +
            "would unintentionally match objects under the longer prefix.",
        );
      }
    }

    // Construct ID collision check
    const constructIds = new Map<string, string>();
    for (const pc of props.prefixes) {
      const tag = prefixToConstructId(pc.s3Prefix);
      const existing = constructIds.get(tag);
      if (existing !== undefined) {
        throw new Error(
          `Prefixes "${existing}" and "${pc.s3Prefix}" produce the same construct ID "${tag}". ` +
            "Use prefixes that produce distinct construct IDs.",
        );
      }
      constructIds.set(tag, pc.s3Prefix);
    }

    // Per-prefix validation (format, retention, OIDC)
    for (const pc of props.prefixes) {
      const pfx = pc.s3Prefix;

      if (pfx && (pfx.startsWith("/") || pfx.includes(".."))) {
        throw new Error(
          `s3Prefix "${pfx}": must not start with "/" or contain ".."`,
        );
      }

      if (pc.logVersionRetention !== undefined) {
        const days = pc.logVersionRetention.toDays();
        if (!Number.isInteger(days) || days <= 0) {
          throw new Error(
            `s3Prefix "${pfx}": logVersionRetention must be expressible as a positive whole number of days`,
          );
        }
      }
      if (
        pc.logVersionsToRetain !== undefined &&
        (pc.logVersionsToRetain < 0 ||
          !Number.isInteger(pc.logVersionsToRetain))
      ) {
        throw new Error(
          `s3Prefix "${pfx}": logVersionsToRetain must be a non-negative integer`,
        );
      }
      if (pc.keyVersionRetention !== undefined) {
        const days = pc.keyVersionRetention.toDays();
        if (!Number.isInteger(days) || days <= 0) {
          throw new Error(
            `s3Prefix "${pfx}": keyVersionRetention must be expressible as a positive whole number of days`,
          );
        }
      }
      if (
        pc.keyVersionsToRetain !== undefined &&
        (pc.keyVersionsToRetain < 0 ||
          !Number.isInteger(pc.keyVersionsToRetain))
      ) {
        throw new Error(
          `s3Prefix "${pfx}": keyVersionsToRetain must be a non-negative integer`,
        );
      }

      if (pc.oidcProviders !== undefined && pc.oidcProviders.length > 0) {
        for (const [i, provider] of pc.oidcProviders.entries()) {
          if (!provider.issuerUrl.startsWith("https://")) {
            throw new Error(
              `s3Prefix "${pfx}": oidcProviders[${i}].issuerUrl must start with "https://", got: ${provider.issuerUrl}`,
            );
          }
          if (provider.clientIds.length === 0) {
            throw new Error(
              `s3Prefix "${pfx}": oidcProviders[${i}].clientIds must contain at least one element`,
            );
          }
        }
      }
    }

    // OIDC cross-prefix conflict check: same issuerUrl must have same clientIds
    const oidcClientIdsByIssuer = new Map<string, string[]>();
    for (const pc of props.prefixes) {
      if (pc.oidcProviders === undefined) continue;
      for (const provider of pc.oidcProviders) {
        const existing = oidcClientIdsByIssuer.get(provider.issuerUrl);
        if (existing !== undefined) {
          const sortedExisting = [...existing].sort();
          const sortedNew = [...provider.clientIds].sort();
          if (
            sortedExisting.length !== sortedNew.length ||
            sortedExisting.some((v, i) => v !== sortedNew[i])
          ) {
            throw new Error(
              `OIDC provider conflict: issuerUrl "${provider.issuerUrl}" is referenced by multiple prefixes ` +
                `with different clientIds. AWS IAM allows only one OIDC provider per issuer URL. ` +
                `Conflicting clientIds: [${existing.join(", ")}] vs [${provider.clientIds.join(", ")}]`,
            );
          }
        } else {
          oidcClientIdsByIssuer.set(provider.issuerUrl, provider.clientIds);
        }
      }
    }

    // ── Lifecycle Rules (aggregated before bucket creation) ──

    const lifecycleRules: s3.LifecycleRule[] = [];

    for (const pc of props.prefixes) {
      const pfx = pc.s3Prefix;
      const tag = prefixToConstructId(pfx);

      if (
        pc.logVersionRetention !== undefined ||
        pc.logVersionsToRetain !== undefined
      ) {
        lifecycleRules.push({
          id: `delete-old-log-versions-${tag}`,
          enabled: true,
          noncurrentVersionExpiration: pc.logVersionRetention,
          noncurrentVersionsToRetain: pc.logVersionsToRetain,
          prefix: `${pfx}${LOG_FILE_PATTERN}`,
        });
      }

      if (
        pc.keyVersionRetention !== undefined ||
        pc.keyVersionsToRetain !== undefined
      ) {
        lifecycleRules.push({
          id: `delete-old-key-versions-${tag}`,
          enabled: true,
          noncurrentVersionExpiration: pc.keyVersionRetention,
          noncurrentVersionsToRetain: pc.keyVersionsToRetain,
          prefix: `${pfx}${KEYS_PREFIX}`,
        });
      }
    }

    // ── Bucket Creation ──

    this.bucket = new s3.Bucket(this, "ImmuKVBucket", {
      bucketName: props.bucketName,
      versioned: true,
      encryption: props.useKmsEncryption
        ? s3.BucketEncryption.KMS_MANAGED
        : s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: lifecycleRules.length > 0 ? lifecycleRules : undefined,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ── OIDC Provider Deduplication ──
    // AWS IAM OIDC providers are account-level singletons keyed by issuer URL.
    // Create each provider once, then share across prefixes.

    const oidcProviderMap = new Map<string, iam.OpenIdConnectProvider>();
    let oidcProviderIndex = 0;
    for (const pc of props.prefixes) {
      if (pc.oidcProviders === undefined) continue;
      for (const provider of pc.oidcProviders) {
        if (!oidcProviderMap.has(provider.issuerUrl)) {
          oidcProviderMap.set(
            provider.issuerUrl,
            new iam.OpenIdConnectProvider(
              this,
              `OidcProvider-${oidcProviderIndex}`,
              {
                url: provider.issuerUrl,
                clientIds: provider.clientIds,
              },
            ),
          );
          oidcProviderIndex++;
        }
      }
    }

    // ── Per-Prefix Resource Loop ──

    const resourceMap: { [key: string]: ImmuKVPrefixResources } = {};

    for (const pc of props.prefixes) {
      const pfx = pc.s3Prefix;
      const tag = prefixToConstructId(pfx);

      // IAM: prefix-scoped object ARN
      const objectArn = pfx
        ? `${this.bucket.bucketArn}/${pfx}*`
        : `${this.bucket.bucketArn}/*`;

      const listCondition = pfx
        ? { StringLike: { "s3:prefix": `${pfx}*` } }
        : undefined;

      const readWritePolicy = new iam.ManagedPolicy(
        this,
        `ReadWritePolicy-${tag}`,
        {
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:HeadObject",
              ],
              resources: [objectArn],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["s3:ListBucket", "s3:ListBucketVersions"],
              resources: [this.bucket.bucketArn],
              conditions: listCondition,
            }),
          ],
        },
      );

      const readOnlyPolicy = new iam.ManagedPolicy(
        this,
        `ReadOnlyPolicy-${tag}`,
        {
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["s3:GetObject", "s3:GetObjectVersion", "s3:HeadObject"],
              resources: [objectArn],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["s3:ListBucket", "s3:ListBucketVersions"],
              resources: [this.bucket.bucketArn],
              conditions: listCondition,
            }),
          ],
        },
      );

      // S3 notification
      if (pc.onLogEntryCreated) {
        this.bucket.addEventNotification(
          s3.EventType.OBJECT_CREATED,
          pc.onLogEntryCreated,
          { prefix: `${pfx}${LOG_FILE_PATTERN}` },
        );
      }

      // OIDC federation (providers are shared; roles are per-prefix)
      let federatedRole: iam.Role | undefined;

      if (pc.oidcProviders !== undefined && pc.oidcProviders.length > 0) {
        const resolvedProviders = pc.oidcProviders.map(
          (p) => oidcProviderMap.get(p.issuerUrl)!,
        );

        federatedRole = new iam.Role(this, `FederatedRole-${tag}`, {
          assumedBy: new iam.CompositePrincipal(
            ...resolvedProviders.map(
              (provider, i) =>
                new iam.WebIdentityPrincipal(
                  provider.openIdConnectProviderArn,
                  {
                    StringEquals: {
                      [`${pc.oidcProviders![i]!.issuerUrl.replace(/^https?:\/\//, "")}:aud`]:
                        pc.oidcProviders![i]!.clientIds,
                    },
                  },
                ),
            ),
          ),
          maxSessionDuration: cdk.Duration.hours(1),
        });

        federatedRole.addManagedPolicy(
          pc.oidcReadOnly === true ? readOnlyPolicy : readWritePolicy,
        );
      }

      resourceMap[pfx] = {
        s3Prefix: pfx,
        readWritePolicy,
        readOnlyPolicy,
        federatedRole,
      };
    }

    this.prefixes = resourceMap;
  }
}
