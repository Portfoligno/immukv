import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Template, Match } from "aws-cdk-lib/assertions";
import { ImmuKV } from "../src/immukv";

describe("ImmuKV", () => {
  let app: cdk.App;
  let stack: cdk.Stack;

  beforeEach(() => {
    app = new cdk.App();
    stack = new cdk.Stack(app, "TestStack");
  });

  describe("Basic Construct Creation", () => {
    test("creates S3 bucket with versioning enabled", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        VersioningConfiguration: {
          Status: "Enabled",
        },
      });
    });

    test("creates S3 bucket with S3-managed encryption by default", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        BucketEncryption: {
          ServerSideEncryptionConfiguration: [
            {
              ServerSideEncryptionByDefault: {
                SSEAlgorithm: "AES256",
              },
            },
          ],
        },
      });
    });

    test("creates S3 bucket with KMS encryption when enabled", () => {
      new ImmuKV(stack, "ImmuKV", {
        useKmsEncryption: true,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        BucketEncryption: {
          ServerSideEncryptionConfiguration: [
            {
              ServerSideEncryptionByDefault: {
                SSEAlgorithm: "aws:kms",
              },
            },
          ],
        },
      });
    });

    test("creates S3 bucket with custom name", () => {
      new ImmuKV(stack, "ImmuKV", {
        bucketName: "my-custom-bucket",
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        BucketName: "my-custom-bucket",
      });
    });

    test("creates S3 bucket with public access blocked", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        PublicAccessBlockConfiguration: {
          BlockPublicAcls: true,
          BlockPublicPolicy: true,
          IgnorePublicAcls: true,
          RestrictPublicBuckets: true,
        },
      });
    });

    test("creates exactly one S3 bucket", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::S3::Bucket", 1);
    });
  });

  describe("Lifecycle Rules", () => {
    test("does not create lifecycle rules by default (unlimited retention)", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: Match.absent(),
      });
    });

    test("creates lifecycle rule for log versions when explicitly configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        logVersionRetention: cdk.Duration.days(365),
        logVersionsToRetain: 1000,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions",
              Status: "Enabled",
              NoncurrentVersionExpiration: {
                NoncurrentDays: 365,
              },
              NoncurrentVersionTransitions: Match.absent(),
              Prefix: "_log.json",
            }),
          ]),
        },
      });
    });

    test("creates lifecycle rule for key versions when explicitly configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        keyVersionRetention: cdk.Duration.days(365),
        keyVersionsToRetain: 100,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions",
              Status: "Enabled",
              NoncurrentVersionExpiration: {
                NoncurrentDays: 365,
              },
              NoncurrentVersionTransitions: Match.absent(),
              Prefix: "keys/",
            }),
          ]),
        },
      });
    });

    test("respects custom log retention settings", () => {
      new ImmuKV(stack, "ImmuKV", {
        logVersionRetention: cdk.Duration.days(180),
        logVersionsToRetain: 500,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions",
              NoncurrentVersionExpiration: {
                NoncurrentDays: 180,
                NewerNoncurrentVersions: 500,
              },
            }),
          ]),
        },
      });
    });

    test("respects custom key retention settings", () => {
      new ImmuKV(stack, "ImmuKV", {
        keyVersionRetention: cdk.Duration.days(90),
        keyVersionsToRetain: 50,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions",
              NoncurrentVersionExpiration: {
                NoncurrentDays: 90,
                NewerNoncurrentVersions: 50,
              },
            }),
          ]),
        },
      });
    });

    test("allows only retention days (unlimited version count)", () => {
      new ImmuKV(stack, "ImmuKV", {
        logVersionRetention: cdk.Duration.days(180),
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions",
              NoncurrentVersionExpiration: {
                NoncurrentDays: 180,
              },
            }),
          ]),
        },
      });
    });

    test("allows only version count (unlimited retention time)", () => {
      new ImmuKV(stack, "ImmuKV", {
        keyVersionsToRetain: 50,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions",
              NoncurrentVersionExpiration: Match.absent(),
            }),
          ]),
        },
      });
    });

    test("applies s3Prefix to lifecycle rules when configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        s3Prefix: "myapp/",
        logVersionRetention: cdk.Duration.days(365),
        logVersionsToRetain: 1000,
        keyVersionRetention: cdk.Duration.days(365),
        keyVersionsToRetain: 100,
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions",
              Prefix: "myapp/_log.json",
            }),
            Match.objectLike({
              Id: "delete-old-key-versions",
              Prefix: "myapp/keys/",
            }),
          ]),
        },
      });
    });
  });

  describe("IAM Policies", () => {
    test("creates read-write IAM policy", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::IAM::ManagedPolicy", {
        PolicyDocument: {
          Statement: Match.arrayWith([
            Match.objectLike({
              Effect: "Allow",
              Action: [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:HeadObject",
              ],
            }),
          ]),
        },
      });
    });

    test("creates read-only IAM policy", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::IAM::ManagedPolicy", {
        PolicyDocument: {
          Statement: Match.arrayWith([
            Match.objectLike({
              Effect: "Allow",
              Action: [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:HeadObject",
              ],
            }),
          ]),
        },
      });
    });

    test("creates exactly two IAM policies", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::IAM::ManagedPolicy", 2);
    });
  });

  describe("S3 Event Notifications", () => {
    test("does not create Lambda permission when no notification configured", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::Lambda::Permission", 0);
    });

    test("configures Lambda notification when provided via onLogEntryCreated property", () => {
      // Create Lambda in a separate stack (cross-stack pattern)
      const lambdaStack = new cdk.Stack(app, "LambdaStack");
      const testFn = new lambda.Function(lambdaStack, "TestFunction", {
        runtime: lambda.Runtime.PYTHON_3_11,
        handler: "index.handler",
        code: lambda.Code.fromInline("def handler(event, context): pass"),
      });

      // Create ImmuKV construct with Lambda notification via the onLogEntryCreated property
      new ImmuKV(stack, "ImmuKV", {
        onLogEntryCreated: new s3n.LambdaDestination(testFn),
      });

      const template = Template.fromStack(stack);

      // Should create S3 notification configuration
      template.hasResourceProperties("Custom::S3BucketNotifications", {
        NotificationConfiguration: {
          LambdaFunctionConfigurations: Match.arrayWith([
            Match.objectLike({
              Events: ["s3:ObjectCreated:*"],
            }),
          ]),
        },
      });
    });

    test("configures SNS notification when provided via onLogEntryCreated property", () => {
      // Create SNS topic in the same stack
      const testTopic = new sns.Topic(stack, "TestTopic");

      // Create ImmuKV construct with SNS notification via the onLogEntryCreated property
      new ImmuKV(stack, "ImmuKV", {
        onLogEntryCreated: new s3n.SnsDestination(testTopic),
      });

      const template = Template.fromStack(stack);

      // Should configure S3 bucket notifications
      template.hasResourceProperties("Custom::S3BucketNotifications", {
        NotificationConfiguration: {
          TopicConfigurations: Match.arrayWith([
            Match.objectLike({
              Events: ["s3:ObjectCreated:*"],
            }),
          ]),
        },
      });
    });

    test("configures SQS notification when provided via onLogEntryCreated property", () => {
      // Create SQS queue in the same stack
      const testQueue = new sqs.Queue(stack, "TestQueue");

      // Create ImmuKV construct with SQS notification via the onLogEntryCreated property
      new ImmuKV(stack, "ImmuKV", {
        onLogEntryCreated: new s3n.SqsDestination(testQueue),
      });

      const template = Template.fromStack(stack);

      // Should configure S3 bucket notifications
      template.hasResourceProperties("Custom::S3BucketNotifications", {
        NotificationConfiguration: {
          QueueConfigurations: Match.arrayWith([
            Match.objectLike({
              Events: ["s3:ObjectCreated:*"],
            }),
          ]),
        },
      });
    });

    test("notification respects s3Prefix", () => {
      const testFn = new lambda.Function(stack, "TestFunction", {
        runtime: lambda.Runtime.PYTHON_3_11,
        handler: "index.handler",
        code: lambda.Code.fromInline("def handler(event, context): pass"),
      });

      new ImmuKV(stack, "ImmuKV", {
        s3Prefix: "myapp/",
        onLogEntryCreated: new s3n.LambdaDestination(testFn),
      });

      const template = Template.fromStack(stack);

      // The notification configuration should include the prefix filter
      template.hasResourceProperties("Custom::S3BucketNotifications", {
        NotificationConfiguration: {
          LambdaFunctionConfigurations: [
            Match.objectLike({
              Events: ["s3:ObjectCreated:*"],
              Filter: {
                Key: {
                  FilterRules: [
                    {
                      Name: "prefix",
                      Value: "myapp/_log.json",
                    },
                  ],
                },
              },
            }),
          ],
        },
      });
    });
  });

  describe("Public Properties", () => {
    test("exposes bucket as public property", () => {
      const immukv = new ImmuKV(stack, "ImmuKV");

      expect(immukv.bucket).toBeDefined();
      expect(immukv.bucket).toBeInstanceOf(s3.Bucket);
    });

    test("exposes readWritePolicy as public property", () => {
      const immukv = new ImmuKV(stack, "ImmuKV");

      expect(immukv.readWritePolicy).toBeDefined();
    });

    test("exposes readOnlyPolicy as public property", () => {
      const immukv = new ImmuKV(stack, "ImmuKV");

      expect(immukv.readOnlyPolicy).toBeDefined();
    });
  });

  describe("Input Validation", () => {
    test("throws error when s3Prefix starts with /", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          s3Prefix: "/invalid",
        });
      }).toThrow('s3Prefix must not start with "/" or contain ".."');
    });

    test("throws error when s3Prefix contains ..", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          s3Prefix: "invalid/../path",
        });
      }).toThrow('s3Prefix must not start with "/" or contain ".."');
    });

    test("accepts s3Prefix ending with / (directory prefix)", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          s3Prefix: "myapp/",
        });
      }).not.toThrow();
    });

    test("accepts s3Prefix without trailing slash (flat prefix)", () => {
      new ImmuKV(stack, "ImmuKV-Flat", {
        s3Prefix: "myapp",
        logVersionRetention: cdk.Duration.days(365),
        logVersionsToRetain: 1000,
      });
      const template = Template.fromStack(stack);

      // Flat prefix produces "myapp_log.json" (no separator)
      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Prefix: "myapp_log.json",
            }),
          ]),
        },
      });
    });

    test("accepts valid s3Prefix", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          s3Prefix: "valid-prefix",
        });
      }).not.toThrow();
    });

    test("accepts empty s3Prefix", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          s3Prefix: "",
        });
      }).not.toThrow();
    });
  });

  describe("Retention Parameter Validation", () => {
    test("throws error when logVersionRetention is zero days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionRetention: cdk.Duration.days(0),
        });
      }).toThrow(
        "logVersionRetention must be expressible as a positive whole number of days",
      );
    });

    test("throws error when logVersionRetention is negative days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionRetention: cdk.Duration.days(-1),
        });
      }).toThrow("Duration amounts cannot be negative");
    });

    test("throws error when logVersionRetention has fractional days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionRetention: cdk.Duration.hours(36), // 1.5 days
        });
      }).toThrow("cannot be converted into a whole number of days");
    });

    test("throws error when logVersionsToRetain is negative", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionsToRetain: -1,
        });
      }).toThrow("logVersionsToRetain must be a non-negative integer");
    });

    test("throws error when logVersionsToRetain is not an integer", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionsToRetain: 10.5,
        });
      }).toThrow("logVersionsToRetain must be a non-negative integer");
    });

    test("accepts zero for logVersionsToRetain", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionsToRetain: 0,
        });
      }).not.toThrow();
    });

    test("throws error when keyVersionRetention is zero days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionRetention: cdk.Duration.days(0),
        });
      }).toThrow(
        "keyVersionRetention must be expressible as a positive whole number of days",
      );
    });

    test("throws error when keyVersionRetention is negative days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionRetention: cdk.Duration.days(-1),
        });
      }).toThrow("Duration amounts cannot be negative");
    });

    test("throws error when keyVersionRetention has fractional days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionRetention: cdk.Duration.hours(73), // 3.04 days (fractional)
        });
      }).toThrow("cannot be converted into a whole number of days");
    });

    test("throws error when keyVersionsToRetain is negative", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionsToRetain: -1,
        });
      }).toThrow("keyVersionsToRetain must be a non-negative integer");
    });

    test("throws error when keyVersionsToRetain is not an integer", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionsToRetain: 5.5,
        });
      }).toThrow("keyVersionsToRetain must be a non-negative integer");
    });

    test("accepts zero for keyVersionsToRetain", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          keyVersionsToRetain: 0,
        });
      }).not.toThrow();
    });

    test("accepts positive retention values", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          logVersionRetention: cdk.Duration.days(90),
          logVersionsToRetain: 500,
          keyVersionRetention: cdk.Duration.days(30),
          keyVersionsToRetain: 50,
        });
      }).not.toThrow();
    });
  });

  describe("OIDC Federation", () => {
    test("creates OIDC provider resource when oidcProviders is specified", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["my-client-id.apps.googleusercontent.com"],
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties(
        "Custom::AWSCDKOpenIdConnectProvider",
        Match.objectLike({
          Url: "https://accounts.google.com",
          ClientIDList: ["my-client-id.apps.googleusercontent.com"],
        }),
      );
    });

    test("creates federated role with correct trust policy (hostname-only condition key)", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["my-client-id"],
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::IAM::Role", {
        AssumeRolePolicyDocument: {
          Statement: Match.arrayWith([
            Match.objectLike({
              Action: "sts:AssumeRoleWithWebIdentity",
              Condition: {
                StringEquals: {
                  "accounts.google.com:aud": ["my-client-id"],
                },
              },
              Effect: "Allow",
            }),
          ]),
        },
        MaxSessionDuration: 3600,
      });
    });

    test("attaches read-only policy when oidcReadOnly is true", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["my-client-id"],
          },
        ],
        oidcReadOnly: true,
      });
      const template = Template.fromStack(stack);

      // Find the federated role (identified by WebIdentity trust policy)
      const roles = template.findResources("AWS::IAM::Role");
      const policies = template.findResources("AWS::IAM::ManagedPolicy");
      const federatedRoleEntry = Object.values(roles).find((r) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoleEntry).toBeDefined();

      // Resolve the attached managed policy and verify it is the read-only one
      const roleManagedPolicyArns = federatedRoleEntry!.Properties
        .ManagedPolicyArns as Array<{ Ref: string }>;
      expect(roleManagedPolicyArns.length).toBe(1);
      const attachedPolicy = policies[roleManagedPolicyArns[0]!.Ref];
      const actions =
        attachedPolicy!.Properties.PolicyDocument.Statement[0].Action;
      expect(actions).not.toContain("s3:PutObject");
      expect(actions).toContain("s3:GetObject");
      expect(actions).toContain("s3:ListBucket");
    });

    test("attaches read-write policy by default (oidcReadOnly not set)", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["my-client-id"],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Find the federated role (identified by WebIdentity trust policy)
      const roles = template.findResources("AWS::IAM::Role");
      const policies = template.findResources("AWS::IAM::ManagedPolicy");
      const federatedRoleEntry = Object.values(roles).find((r) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoleEntry).toBeDefined();

      // Resolve the attached managed policy and verify it is the read-write one
      const roleManagedPolicyArns = federatedRoleEntry!.Properties
        .ManagedPolicyArns as Array<{ Ref: string }>;
      expect(roleManagedPolicyArns.length).toBe(1);
      const attachedPolicy = policies[roleManagedPolicyArns[0]!.Ref];
      const actions =
        attachedPolicy!.Properties.PolicyDocument.Statement[0].Action;
      expect(actions).toContain("s3:PutObject");
      expect(actions).toContain("s3:GetObject");
      expect(actions).toContain("s3:ListBucket");
    });

    test("exposes federatedRole property when oidcProviders specified", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["my-client-id"],
          },
        ],
      });

      expect(immukv.federatedRole).toBeDefined();
    });

    test("federatedRole is undefined when oidcProviders is not specified", () => {
      const immukv = new ImmuKV(stack, "ImmuKV");

      expect(immukv.federatedRole).toBeUndefined();
    });

    test("multiple OIDC providers create multiple resources but one shared role", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [
          {
            issuerUrl: "https://accounts.google.com",
            clientIds: ["google-client-id"],
          },
          {
            issuerUrl: "https://login.microsoftonline.com/tenant-id/v2.0",
            clientIds: ["azure-app-id"],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Two OIDC provider resources
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 2);

      // Exactly one federated role (CDK also creates service roles for custom resources)
      const roles = template.findResources("AWS::IAM::Role");
      const federatedRoles = Object.values(roles).filter((r) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoles.length).toBe(1);

      // The role's trust policy should have conditions for both providers
      template.hasResourceProperties("AWS::IAM::Role", {
        AssumeRolePolicyDocument: {
          Statement: Match.arrayWith([
            Match.objectLike({
              Condition: {
                StringEquals: {
                  "accounts.google.com:aud": ["google-client-id"],
                },
              },
            }),
            Match.objectLike({
              Condition: {
                StringEquals: {
                  "login.microsoftonline.com/tenant-id/v2.0:aud": [
                    "azure-app-id",
                  ],
                },
              },
            }),
          ]),
        },
      });
    });

    test("throws error when issuerUrl does not start with https://", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          oidcProviders: [
            {
              issuerUrl: "http://accounts.google.com",
              clientIds: ["my-client-id"],
            },
          ],
        });
      }).toThrow(
        'oidcProviders[0].issuerUrl must start with "https://", got: http://accounts.google.com',
      );
    });

    test("throws error when issuerUrl is a bare string without scheme", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          oidcProviders: [
            {
              issuerUrl: "accounts.google.com",
              clientIds: ["my-client-id"],
            },
          ],
        });
      }).toThrow('oidcProviders[0].issuerUrl must start with "https://"');
    });

    test("throws error when clientIds is empty", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          oidcProviders: [
            {
              issuerUrl: "https://accounts.google.com",
              clientIds: [],
            },
          ],
        });
      }).toThrow(
        "oidcProviders[0].clientIds must contain at least one element",
      );
    });

    test("throws error with correct index for second provider validation failure", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          oidcProviders: [
            {
              issuerUrl: "https://accounts.google.com",
              clientIds: ["valid-id"],
            },
            {
              issuerUrl: "http://invalid.example.com",
              clientIds: ["some-id"],
            },
          ],
        });
      }).toThrow(
        'oidcProviders[1].issuerUrl must start with "https://", got: http://invalid.example.com',
      );
    });

    test("does not create OIDC resources when oidcProviders is not specified", () => {
      new ImmuKV(stack, "ImmuKV");
      const template = Template.fromStack(stack);

      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 0);
      template.resourceCountIs("AWS::IAM::Role", 0);
    });

    test("does not create OIDC resources when oidcProviders is empty array", () => {
      new ImmuKV(stack, "ImmuKV", {
        oidcProviders: [],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 0);
      template.resourceCountIs("AWS::IAM::Role", 0);
    });
  });
});
