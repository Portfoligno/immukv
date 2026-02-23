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
  /**
   * Email addresses allowed to assume the federated role.
   *
   * When provided, the trust policy adds a `StringEquals` condition on
   * `${issuerUrl}:email` listing these addresses, restricting federation
   * to the specified identities. Must be non-empty when provided.
   *
   * When omitted, the trust policy only checks `:aud` (client ID).
   */
  readonly allowedEmails?: string[];
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
 * Configuration for a wildcard-based IAM prefix pattern.
 *
 * Wildcards (`*`, `?`) are supported in IAM policies via `StringLike` conditions
 * and IAM ARN matching, but do NOT work for S3 lifecycle rules, notifications,
 * or listing. This interface therefore only exposes IAM-related options.
 *
 * @example
 *
 * // Scope IAM to any tenant's logs:
 * { pattern: 'tenant-*\/logs/', oidcProviders: [...] }
 */
export interface ImmuKVWildcardPrefixConfig {
  /**
   * Wildcard prefix pattern for IAM scoping.
   *
   * Supports `*` (matches zero or more characters) and `?` (matches exactly one character).
   * Used in IAM policy resource ARNs and `StringLike` conditions on `s3:prefix`.
   *
   * Validation:
   * - Must not be empty
   * - Must not start with `/` or contain `..`
   * - Must not duplicate another wildcard pattern in the same instance
   */
  readonly pattern: string;

  /**
   * OIDC identity providers for web identity federation scoped to this wildcard pattern.
   *
   * Creates a federated IAM role whose policies are scoped to this pattern only.
   * The role receives this pattern's readWritePolicy by default,
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

/**
 * Resources created for a single wildcard prefix pattern.
 */
export interface ImmuKVWildcardPrefixResources {
  /** The wildcard pattern string (as provided in the config) */
  readonly pattern: string;

  /**
   * IAM managed policy granting read-write access scoped to this wildcard pattern.
   *
   * Object actions (GetObject, GetObjectVersion, PutObject, HeadObject)
   * on `bucketArn/${pattern}*`. Bucket actions (ListBucket, ListBucketVersions)
   * on `bucketArn` with `StringLike` condition on `s3:prefix`.
   */
  readonly readWritePolicy: iam.ManagedPolicy;

  /**
   * IAM managed policy granting read-only access scoped to this wildcard pattern.
   * Same as readWritePolicy but without PutObject.
   */
  readonly readOnlyPolicy: iam.ManagedPolicy;

  /**
   * Federated IAM role for OIDC users scoped to this wildcard pattern.
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

  /**
   * Wildcard prefix configurations for IAM-only scoping.
   *
   * Each entry defines IAM policies using wildcard patterns (e.g., `tenant-*\/logs/`).
   * Wildcards work in IAM via `StringLike` conditions and ARN matching but do NOT
   * support S3 lifecycle rules, notifications, or listing.
   *
   * @default undefined — no wildcard prefixes
   *
   * @example
   *
   * new ImmuKV(this, 'Store', {
   *   prefixes: [{ s3Prefix: '' }],
   *   wildcardPrefixes: [
   *     { pattern: 'tenant-*\/logs/' },
   *     { pattern: '*\/config/', oidcReadOnly: true, oidcProviders: [...] },
   *   ],
   * });
   */
  readonly wildcardPrefixes?: ImmuKVWildcardPrefixConfig[];
}

/**
 * Normalizes an OIDC issuer URL for use as a deduplication key.
 *
 * The URL constructor reliably lowercases the hostname, removes the
 * default HTTPS port (:443), and normalizes the path (adds trailing
 * slash to bare origins). This prevents deploy-time collisions in
 * AWS IAM where variants like `https://Accounts.Google.Com:443` and
 * `https://accounts.google.com/` would be treated as different
 * providers during synth but collide at the API level.
 */
function normalizeIssuerUrl(issuerUrl: string): string {
  return new URL(issuerUrl).href;
}

/**
 * Strips the leading "https://" or "http://" protocol from a URL string.
 */
function stripProtocol(url: string): string {
  if (url.startsWith("https://")) return url.slice(8);
  if (url.startsWith("http://")) return url.slice(7);
  return url;
}

/**
 * Converts an S3 prefix string to a CDK construct-safe ID suffix.
 */
function prefixToConstructId(s3Prefix: string): string {
  if (s3Prefix === "") return "Root";

  // Strip trailing slashes (was: .replace(/\/+$/, ""))
  let stripped = s3Prefix;
  while (stripped.endsWith("/")) {
    stripped = stripped.slice(0, -1);
  }

  // Replace non-alphanumeric characters with spaces (was: .replace(/[^a-zA-Z0-9]/g, " "))
  let spaced = "";
  for (const ch of stripped) {
    if (
      (ch >= "a" && ch <= "z") ||
      (ch >= "A" && ch <= "Z") ||
      (ch >= "0" && ch <= "9")
    ) {
      spaced += ch;
    } else {
      spaced += " ";
    }
  }

  // Split on whitespace runs and PascalCase-join (was: .split(/\s+/))
  return spaced
    .trim()
    .split(" ")
    .filter((w) => w !== "")
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
   * Per-wildcard-pattern resources, keyed by the pattern string.
   * Use `wildcardPrefix()` for type-safe access with runtime validation.
   */
  public readonly wildcardPrefixes: {
    [key: string]: ImmuKVWildcardPrefixResources;
  };

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

  /**
   * Get resources for a specific wildcard prefix pattern.
   * @throws Error if no wildcard prefix with that pattern exists.
   *
   * @example
   * instance.wildcardPrefix("tenant-*\/logs/").readWritePolicy
   */
  public wildcardPrefix(pattern: string): ImmuKVWildcardPrefixResources {
    const resources = this.wildcardPrefixes[pattern];
    if (!resources) {
      throw new Error(
        `No wildcard prefix "${pattern}" configured. Available: ${Object.keys(
          this.wildcardPrefixes,
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
          if (
            provider.allowedEmails !== undefined &&
            provider.allowedEmails.length === 0
          ) {
            throw new Error(
              `s3Prefix "${pfx}": oidcProviders[${i}].allowedEmails must be non-empty when provided`,
            );
          }
        }
      }
    }

    // ── Wildcard Prefix Validation ──

    const wildcardConfigs = props.wildcardPrefixes ?? [];

    {
      // Duplicate check
      const seenWildcard = new Set<string>();
      for (const wc of wildcardConfigs) {
        if (wc.pattern === "") {
          throw new Error("Wildcard prefix pattern must not be empty");
        }
        if (wc.pattern.startsWith("/") || wc.pattern.includes("..")) {
          throw new Error(
            `Wildcard pattern "${wc.pattern}": must not start with "/" or contain ".."`,
          );
        }
        if (seenWildcard.has(wc.pattern)) {
          throw new Error(`Duplicate wildcard pattern: "${wc.pattern}"`);
        }
        seenWildcard.add(wc.pattern);
      }

      // Construct ID collision check (among wildcard prefixes)
      const wildcardConstructIds = new Map<string, string>();
      for (const wc of wildcardConfigs) {
        const tag = prefixToConstructId(wc.pattern);
        const existing = wildcardConstructIds.get(tag);
        if (existing !== undefined) {
          throw new Error(
            `Wildcard patterns "${existing}" and "${wc.pattern}" produce the same construct ID "${tag}". ` +
              "Use patterns that produce distinct construct IDs.",
          );
        }
        wildcardConstructIds.set(tag, wc.pattern);
      }

      // Construct ID collision check (wildcard vs literal prefixes)
      for (const wc of wildcardConfigs) {
        const tag = prefixToConstructId(wc.pattern);
        const literalCollision = constructIds.get(tag);
        if (literalCollision !== undefined) {
          throw new Error(
            `Wildcard pattern "${wc.pattern}" and literal prefix "${literalCollision}" produce the same construct ID "${tag}". ` +
              "Use names that produce distinct construct IDs.",
          );
        }
      }

      // Per-wildcard OIDC validation
      for (const wc of wildcardConfigs) {
        if (wc.oidcProviders !== undefined && wc.oidcProviders.length > 0) {
          for (const [i, provider] of wc.oidcProviders.entries()) {
            if (!provider.issuerUrl.startsWith("https://")) {
              throw new Error(
                `Wildcard pattern "${wc.pattern}": oidcProviders[${i}].issuerUrl must start with "https://", got: ${provider.issuerUrl}`,
              );
            }
            if (provider.clientIds.length === 0) {
              throw new Error(
                `Wildcard pattern "${wc.pattern}": oidcProviders[${i}].clientIds must contain at least one element`,
              );
            }
            if (
              provider.allowedEmails !== undefined &&
              provider.allowedEmails.length === 0
            ) {
              throw new Error(
                `Wildcard pattern "${wc.pattern}": oidcProviders[${i}].allowedEmails must be non-empty when provided`,
              );
            }
          }
        }
      }
    }

    // OIDC cross-prefix conflict check: same issuerUrl must have same clientIds.
    // Normalize issuer URLs so that cosmetic variants (trailing slash,
    // mixed case, default port) are detected as the same provider.
    // Includes both literal and wildcard prefix providers.
    const oidcClientIdsByIssuer = new Map<string, string[]>();

    const allOidcSources: { label: string; providers: OidcProvider[] }[] = [];
    for (const pc of props.prefixes) {
      if (pc.oidcProviders !== undefined) {
        allOidcSources.push({
          label: `prefix "${pc.s3Prefix}"`,
          providers: pc.oidcProviders,
        });
      }
    }
    for (const wc of wildcardConfigs) {
      if (wc.oidcProviders !== undefined) {
        allOidcSources.push({
          label: `wildcard pattern "${wc.pattern}"`,
          providers: wc.oidcProviders,
        });
      }
    }

    for (const source of allOidcSources) {
      for (const provider of source.providers) {
        const normalizedUrl = normalizeIssuerUrl(provider.issuerUrl);
        const existing = oidcClientIdsByIssuer.get(normalizedUrl);
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
          oidcClientIdsByIssuer.set(normalizedUrl, provider.clientIds);
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
    // Create each provider once, then share across literal and wildcard prefixes.

    const oidcProviderMap = new Map<string, iam.OpenIdConnectProvider>();
    let oidcProviderIndex = 0;
    for (const source of allOidcSources) {
      for (const provider of source.providers) {
        const normalizedUrl = normalizeIssuerUrl(provider.issuerUrl);
        if (!oidcProviderMap.has(normalizedUrl)) {
          oidcProviderMap.set(
            normalizedUrl,
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
          (p) => oidcProviderMap.get(normalizeIssuerUrl(p.issuerUrl))!,
        );

        federatedRole = new iam.Role(this, `FederatedRole-${tag}`, {
          assumedBy: new iam.CompositePrincipal(
            ...resolvedProviders.map((provider, i) => {
              const oidc = pc.oidcProviders![i]!;
              const issuerHost = stripProtocol(oidc.issuerUrl);
              const conditions: Record<string, string[]> = {
                [`${issuerHost}:aud`]: oidc.clientIds,
              };
              if (oidc.allowedEmails !== undefined) {
                conditions[`${issuerHost}:email`] = oidc.allowedEmails;
              }
              return new iam.WebIdentityPrincipal(
                provider.openIdConnectProviderArn,
                { StringEquals: conditions },
              );
            }),
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

    // ── Per-Wildcard-Prefix Resource Loop ──

    const wildcardResourceMap: {
      [key: string]: ImmuKVWildcardPrefixResources;
    } = {};

    for (const wc of wildcardConfigs) {
      const pat = wc.pattern;
      const tag = prefixToConstructId(pat);

      // IAM: wildcard-scoped object ARN — wildcards are native to IAM ARN matching
      const objectArn = `${this.bucket.bucketArn}/${pat}*`;

      // StringLike condition for ListBucket — wildcards are native to StringLike
      const listCondition = { StringLike: { "s3:prefix": `${pat}*` } };

      const readWritePolicy = new iam.ManagedPolicy(
        this,
        `WcReadWritePolicy-${tag}`,
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
        `WcReadOnlyPolicy-${tag}`,
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

      // OIDC federation (providers are shared; roles are per-wildcard-prefix)
      let federatedRole: iam.Role | undefined;

      if (wc.oidcProviders !== undefined && wc.oidcProviders.length > 0) {
        const resolvedProviders = wc.oidcProviders.map(
          (p) => oidcProviderMap.get(normalizeIssuerUrl(p.issuerUrl))!,
        );

        federatedRole = new iam.Role(this, `WcFederatedRole-${tag}`, {
          assumedBy: new iam.CompositePrincipal(
            ...resolvedProviders.map((provider, i) => {
              const oidc = wc.oidcProviders![i]!;
              const issuerHost = stripProtocol(oidc.issuerUrl);
              const conditions: Record<string, string[]> = {
                [`${issuerHost}:aud`]: oidc.clientIds,
              };
              if (oidc.allowedEmails !== undefined) {
                conditions[`${issuerHost}:email`] = oidc.allowedEmails;
              }
              return new iam.WebIdentityPrincipal(
                provider.openIdConnectProviderArn,
                { StringEquals: conditions },
              );
            }),
          ),
          maxSessionDuration: cdk.Duration.hours(1),
        });

        federatedRole.addManagedPolicy(
          wc.oidcReadOnly === true ? readOnlyPolicy : readWritePolicy,
        );
      }

      wildcardResourceMap[pat] = {
        pattern: pat,
        readWritePolicy,
        readOnlyPolicy,
        federatedRole,
      };
    }

    this.wildcardPrefixes = wildcardResourceMap;
  }
}
