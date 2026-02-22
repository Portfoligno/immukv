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
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        VersioningConfiguration: {
          Status: "Enabled",
        },
      });
    });

    test("creates S3 bucket with S3-managed encryption by default", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
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
        prefixes: [{ s3Prefix: "" }],
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
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        BucketName: "my-custom-bucket",
      });
    });

    test("creates S3 bucket with public access blocked", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
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
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::S3::Bucket", 1);
    });

    test("creates exactly one S3 bucket even with multiple prefixes", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "pipeline/" }, { s3Prefix: "config/" }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::S3::Bucket", 1);
    });
  });

  describe("Lifecycle Rules", () => {
    test("does not create lifecycle rules by default (unlimited retention)", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: Match.absent(),
      });
    });

    test("creates lifecycle rule for log versions when explicitly configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            logVersionRetention: cdk.Duration.days(365),
            logVersionsToRetain: 1000,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions-Root",
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
        prefixes: [
          {
            s3Prefix: "",
            keyVersionRetention: cdk.Duration.days(365),
            keyVersionsToRetain: 100,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions-Root",
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
        prefixes: [
          {
            s3Prefix: "",
            logVersionRetention: cdk.Duration.days(180),
            logVersionsToRetain: 500,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions-Root",
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
        prefixes: [
          {
            s3Prefix: "",
            keyVersionRetention: cdk.Duration.days(90),
            keyVersionsToRetain: 50,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions-Root",
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
        prefixes: [
          {
            s3Prefix: "",
            logVersionRetention: cdk.Duration.days(180),
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions-Root",
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
        prefixes: [
          {
            s3Prefix: "",
            keyVersionsToRetain: 50,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-key-versions-Root",
              NoncurrentVersionExpiration: Match.absent(),
            }),
          ]),
        },
      });
    });

    test("applies s3Prefix to lifecycle rules when configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "myapp/",
            logVersionRetention: cdk.Duration.days(365),
            logVersionsToRetain: 1000,
            keyVersionRetention: cdk.Duration.days(365),
            keyVersionsToRetain: 100,
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions-Myapp",
              Prefix: "myapp/_log.json",
            }),
            Match.objectLike({
              Id: "delete-old-key-versions-Myapp",
              Prefix: "myapp/keys/",
            }),
          ]),
        },
      });
    });

    test("creates separate lifecycle rules per prefix", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            logVersionRetention: cdk.Duration.days(2555),
          },
          {
            s3Prefix: "config/",
            logVersionRetention: cdk.Duration.days(90),
          },
        ],
      });
      const template = Template.fromStack(stack);

      template.hasResourceProperties("AWS::S3::Bucket", {
        LifecycleConfiguration: {
          Rules: Match.arrayWith([
            Match.objectLike({
              Id: "delete-old-log-versions-Pipeline",
              NoncurrentVersionExpiration: { NoncurrentDays: 2555 },
              Prefix: "pipeline/_log.json",
            }),
            Match.objectLike({
              Id: "delete-old-log-versions-Config",
              NoncurrentVersionExpiration: { NoncurrentDays: 90 },
              Prefix: "config/_log.json",
            }),
          ]),
        },
      });
    });
  });

  describe("IAM Policies", () => {
    test("creates read-write IAM policy with two statements for root prefix", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      // Read-write policy: object actions + bucket actions in separate statements
      template.hasResourceProperties("AWS::IAM::ManagedPolicy", {
        PolicyDocument: {
          Statement: [
            Match.objectLike({
              Effect: "Allow",
              Action: [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:HeadObject",
              ],
            }),
            Match.objectLike({
              Effect: "Allow",
              Action: ["s3:ListBucket", "s3:ListBucketVersions"],
            }),
          ],
        },
      });
    });

    test("creates read-only IAM policy without PutObject", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      // Read-only policy: no PutObject
      template.hasResourceProperties("AWS::IAM::ManagedPolicy", {
        PolicyDocument: {
          Statement: [
            Match.objectLike({
              Effect: "Allow",
              Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:HeadObject"],
            }),
            Match.objectLike({
              Effect: "Allow",
              Action: ["s3:ListBucket", "s3:ListBucketVersions"],
            }),
          ],
        },
      });
    });

    test("creates exactly two IAM policies for a single prefix", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::IAM::ManagedPolicy", 2);
    });

    test("creates four IAM policies for two prefixes", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "pipeline/" }, { s3Prefix: "config/" }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("AWS::IAM::ManagedPolicy", 4);
    });

    test("root prefix policies have no s3:prefix condition on ListBucket", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      const policies = template.findResources("AWS::IAM::ManagedPolicy");
      for (const policy of Object.values(policies)) {
        const statements = policy.Properties.PolicyDocument.Statement;
        for (const stmt of statements) {
          if (
            Array.isArray(stmt.Action) &&
            stmt.Action.includes("s3:ListBucket")
          ) {
            // Root prefix should have no Condition
            expect(stmt.Condition).toBeUndefined();
          }
        }
      }
    });

    test("non-root prefix policies have s3:prefix condition on ListBucket", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "config/" }],
      });
      const template = Template.fromStack(stack);

      const policies = template.findResources("AWS::IAM::ManagedPolicy");
      for (const policy of Object.values(policies)) {
        const statements = policy.Properties.PolicyDocument.Statement;
        for (const stmt of statements) {
          if (
            Array.isArray(stmt.Action) &&
            stmt.Action.includes("s3:ListBucket")
          ) {
            expect(stmt.Condition).toEqual({
              StringLike: { "s3:prefix": "config/*" },
            });
          }
        }
      }
    });
  });

  describe("S3 Event Notifications", () => {
    test("does not create Lambda permission when no notification configured", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
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

      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            onLogEntryCreated: new s3n.LambdaDestination(testFn),
          },
        ],
      });

      const template = Template.fromStack(stack);

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
      const testTopic = new sns.Topic(stack, "TestTopic");

      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            onLogEntryCreated: new s3n.SnsDestination(testTopic),
          },
        ],
      });

      const template = Template.fromStack(stack);

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
      const testQueue = new sqs.Queue(stack, "TestQueue");

      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            onLogEntryCreated: new s3n.SqsDestination(testQueue),
          },
        ],
      });

      const template = Template.fromStack(stack);

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
        prefixes: [
          {
            s3Prefix: "myapp/",
            onLogEntryCreated: new s3n.LambdaDestination(testFn),
          },
        ],
      });

      const template = Template.fromStack(stack);

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

    test("each prefix can have its own notification destination", () => {
      const fn1 = new lambda.Function(stack, "Fn1", {
        runtime: lambda.Runtime.PYTHON_3_11,
        handler: "index.handler",
        code: lambda.Code.fromInline("def handler(event, context): pass"),
      });
      const fn2 = new lambda.Function(stack, "Fn2", {
        runtime: lambda.Runtime.PYTHON_3_11,
        handler: "index.handler",
        code: lambda.Code.fromInline("def handler(event, context): pass"),
      });

      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            onLogEntryCreated: new s3n.LambdaDestination(fn1),
          },
          {
            s3Prefix: "config/",
            onLogEntryCreated: new s3n.LambdaDestination(fn2),
          },
        ],
      });

      const template = Template.fromStack(stack);

      template.hasResourceProperties("Custom::S3BucketNotifications", {
        NotificationConfiguration: {
          LambdaFunctionConfigurations: Match.arrayWith([
            Match.objectLike({
              Filter: {
                Key: {
                  FilterRules: [
                    { Name: "prefix", Value: "pipeline/_log.json" },
                  ],
                },
              },
            }),
            Match.objectLike({
              Filter: {
                Key: {
                  FilterRules: [{ Name: "prefix", Value: "config/_log.json" }],
                },
              },
            }),
          ]),
        },
      });
    });
  });

  describe("Public Properties", () => {
    test("exposes bucket as public property", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });

      expect(immukv.bucket).toBeDefined();
      expect(immukv.bucket).toBeInstanceOf(s3.Bucket);
    });

    test("exposes prefixes object", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });

      expect(immukv.prefixes).toBeDefined();
      expect(Object.keys(immukv.prefixes).length).toBe(1);
      expect("" in immukv.prefixes).toBe(true);
    });

    test("prefixes object contains correct keys for multi-prefix", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "pipeline/" }, { s3Prefix: "config/" }],
      });

      expect(Object.keys(immukv.prefixes).length).toBe(2);
      expect("pipeline/" in immukv.prefixes).toBe(true);
      expect("config/" in immukv.prefixes).toBe(true);
    });

    test("prefix() returns resources for valid prefix", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });

      const resources = immukv.prefix("");
      expect(resources.s3Prefix).toBe("");
      expect(resources.readWritePolicy).toBeDefined();
      expect(resources.readOnlyPolicy).toBeDefined();
    });

    test("prefix() throws for non-existent prefix", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "pipeline/" }],
      });

      expect(() => immukv.prefix("config/")).toThrow(
        'No prefix "config/" configured. Available: "pipeline/"',
      );
    });

    test("prefix resources include s3Prefix string", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "config/" }],
      });

      expect(immukv.prefix("config/").s3Prefix).toBe("config/");
    });
  });

  describe("Input Validation", () => {
    test("throws error when prefixes array is empty", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [],
        });
      }).toThrow("prefixes must contain at least one element");
    });

    test("throws error when s3Prefix starts with /", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "/invalid" }],
        });
      }).toThrow(
        's3Prefix "/invalid": must not start with "/" or contain ".."',
      );
    });

    test("throws error when s3Prefix contains ..", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "invalid/../path" }],
        });
      }).toThrow(
        's3Prefix "invalid/../path": must not start with "/" or contain ".."',
      );
    });

    test("accepts s3Prefix ending with / (directory prefix)", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "myapp/" }],
        });
      }).not.toThrow();
    });

    test("accepts s3Prefix without trailing slash (flat prefix)", () => {
      new ImmuKV(stack, "ImmuKV-Flat", {
        prefixes: [
          {
            s3Prefix: "myapp",
            logVersionRetention: cdk.Duration.days(365),
            logVersionsToRetain: 1000,
          },
        ],
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
          prefixes: [{ s3Prefix: "valid-prefix" }],
        });
      }).not.toThrow();
    });

    test("accepts empty s3Prefix", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "" }],
        });
      }).not.toThrow();
    });
  });

  describe("Prefix Overlap Validation", () => {
    test("throws error for duplicate prefixes", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "config/" }, { s3Prefix: "config/" }],
        });
      }).toThrow('Duplicate prefix: "config/"');
    });

    test("throws error when empty prefix coexists with others", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "" }, { s3Prefix: "config/" }],
        });
      }).toThrow('Empty-string prefix ("") cannot coexist with other prefixes');
    });

    test("throws error for overlapping prefixes (parent/child)", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "data/" }, { s3Prefix: "data/subset/" }],
        });
      }).toThrow('Overlapping prefixes: "data/" is a prefix of "data/subset/"');
    });

    test("allows non-overlapping prefixes", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "pipeline/" }, { s3Prefix: "config/" }],
        });
      }).not.toThrow();
    });

    test('allows "a/" and "ab/" (not overlapping)', () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "a/" }, { s3Prefix: "ab/" }],
        });
      }).not.toThrow();
    });

    test("allows single root prefix alone", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "" }],
        });
      }).not.toThrow();
    });

    test("throws error for construct ID collision", () => {
      // Both would produce the same construct ID
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [{ s3Prefix: "my-app/" }, { s3Prefix: "my_app/" }],
        });
      }).toThrow('produce the same construct ID "MyApp"');
    });
  });

  describe("Retention Parameter Validation", () => {
    test("throws error when logVersionRetention is zero days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionRetention: cdk.Duration.days(0),
            },
          ],
        });
      }).toThrow(
        "logVersionRetention must be expressible as a positive whole number of days",
      );
    });

    test("throws error when logVersionRetention is negative days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionRetention: cdk.Duration.days(-1),
            },
          ],
        });
      }).toThrow("Duration amounts cannot be negative");
    });

    test("throws error when logVersionRetention has fractional days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionRetention: cdk.Duration.hours(36), // 1.5 days
            },
          ],
        });
      }).toThrow("cannot be converted into a whole number of days");
    });

    test("throws error when logVersionsToRetain is negative", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionsToRetain: -1,
            },
          ],
        });
      }).toThrow("logVersionsToRetain must be a non-negative integer");
    });

    test("throws error when logVersionsToRetain is not an integer", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionsToRetain: 10.5,
            },
          ],
        });
      }).toThrow("logVersionsToRetain must be a non-negative integer");
    });

    test("accepts zero for logVersionsToRetain", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionsToRetain: 0,
            },
          ],
        });
      }).not.toThrow();
    });

    test("throws error when keyVersionRetention is zero days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionRetention: cdk.Duration.days(0),
            },
          ],
        });
      }).toThrow(
        "keyVersionRetention must be expressible as a positive whole number of days",
      );
    });

    test("throws error when keyVersionRetention is negative days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionRetention: cdk.Duration.days(-1),
            },
          ],
        });
      }).toThrow("Duration amounts cannot be negative");
    });

    test("throws error when keyVersionRetention has fractional days", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionRetention: cdk.Duration.hours(73), // 3.04 days
            },
          ],
        });
      }).toThrow("cannot be converted into a whole number of days");
    });

    test("throws error when keyVersionsToRetain is negative", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionsToRetain: -1,
            },
          ],
        });
      }).toThrow("keyVersionsToRetain must be a non-negative integer");
    });

    test("throws error when keyVersionsToRetain is not an integer", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionsToRetain: 5.5,
            },
          ],
        });
      }).toThrow("keyVersionsToRetain must be a non-negative integer");
    });

    test("accepts zero for keyVersionsToRetain", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              keyVersionsToRetain: 0,
            },
          ],
        });
      }).not.toThrow();
    });

    test("accepts positive retention values", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              logVersionRetention: cdk.Duration.days(90),
              logVersionsToRetain: 500,
              keyVersionRetention: cdk.Duration.days(30),
              keyVersionsToRetain: 50,
            },
          ],
        });
      }).not.toThrow();
    });

    test("validation error messages include prefix for context", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "myapp/",
              logVersionsToRetain: -1,
            },
          ],
        });
      }).toThrow('s3Prefix "myapp/": logVersionsToRetain');
    });
  });

  describe("OIDC Federation", () => {
    test("creates OIDC provider resource when oidcProviders is specified on a prefix", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["my-client-id.apps.googleusercontent.com"],
              },
            ],
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
        prefixes: [
          {
            s3Prefix: "",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["my-client-id"],
              },
            ],
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
        prefixes: [
          {
            s3Prefix: "",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["my-client-id"],
              },
            ],
            oidcReadOnly: true,
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Find the federated role
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
      // Read-only: first statement should NOT have PutObject
      const firstStatementActions =
        attachedPolicy!.Properties.PolicyDocument.Statement[0].Action;
      expect(firstStatementActions).not.toContain("s3:PutObject");
      expect(firstStatementActions).toContain("s3:GetObject");
    });

    test("attaches read-write policy by default (oidcReadOnly not set)", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["my-client-id"],
              },
            ],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Find the federated role
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
      const firstStatementActions =
        attachedPolicy!.Properties.PolicyDocument.Statement[0].Action;
      expect(firstStatementActions).toContain("s3:PutObject");
      expect(firstStatementActions).toContain("s3:GetObject");
    });

    test("exposes federatedRole property when oidcProviders specified on prefix", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["my-client-id"],
              },
            ],
          },
        ],
      });

      expect(immukv.prefix("").federatedRole).toBeDefined();
    });

    test("federatedRole is undefined when oidcProviders is not specified", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });

      expect(immukv.prefix("").federatedRole).toBeUndefined();
    });

    test("multiple OIDC providers create multiple resources but one shared role per prefix", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "",
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
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Two OIDC provider resources
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 2);

      // Exactly one federated role
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
          prefixes: [
            {
              s3Prefix: "",
              oidcProviders: [
                {
                  issuerUrl: "http://accounts.google.com",
                  clientIds: ["my-client-id"],
                },
              ],
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
          prefixes: [
            {
              s3Prefix: "",
              oidcProviders: [
                {
                  issuerUrl: "accounts.google.com",
                  clientIds: ["my-client-id"],
                },
              ],
            },
          ],
        });
      }).toThrow('oidcProviders[0].issuerUrl must start with "https://"');
    });

    test("throws error when clientIds is empty", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "",
              oidcProviders: [
                {
                  issuerUrl: "https://accounts.google.com",
                  clientIds: [],
                },
              ],
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
          prefixes: [
            {
              s3Prefix: "",
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
            },
          ],
        });
      }).toThrow(
        'oidcProviders[1].issuerUrl must start with "https://", got: http://invalid.example.com',
      );
    });

    test("does not create OIDC resources when oidcProviders is not specified", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "" }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 0);
      template.resourceCountIs("AWS::IAM::Role", 0);
    });

    test("does not create OIDC resources when oidcProviders is empty array", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "", oidcProviders: [] }],
      });
      const template = Template.fromStack(stack);

      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 0);
      template.resourceCountIs("AWS::IAM::Role", 0);
    });

    test("per-prefix OIDC: each prefix gets its own federated role with shared provider", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["shared-client"],
              },
            ],
          },
          {
            s3Prefix: "config/",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["shared-client"],
              },
            ],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Only one OIDC provider (shared across prefixes with same issuerUrl)
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 1);

      // Two federated roles (one per prefix)
      const roles = template.findResources("AWS::IAM::Role");
      const federatedRoles = Object.values(roles).filter((r) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoles.length).toBe(2);
    });

    test("two prefixes, same OIDC provider, different oidcReadOnly: one gets readWrite, other gets readOnly", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["shared-client"],
              },
            ],
            oidcReadOnly: false,
          },
          {
            s3Prefix: "config/",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["shared-client"],
              },
            ],
            oidcReadOnly: true,
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Only one OIDC provider
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 1);

      // Two federated roles
      const roles = template.findResources("AWS::IAM::Role");
      const policies = template.findResources("AWS::IAM::ManagedPolicy");
      const federatedRoles = Object.entries(roles).filter(([, r]) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoles.length).toBe(2);

      // Collect the policy actions attached to each federated role
      const policyActions = federatedRoles.map(([, role]) => {
        const policyArns = role.Properties.ManagedPolicyArns as Array<{
          Ref: string;
        }>;
        const policy = policies[policyArns[0]!.Ref];
        return policy!.Properties.PolicyDocument.Statement[0]
          .Action as string[];
      });

      // One role should have PutObject (read-write), the other should not (read-only)
      const hasReadWrite = policyActions.some((actions) =>
        actions.includes("s3:PutObject"),
      );
      const hasReadOnly = policyActions.some(
        (actions) => !actions.includes("s3:PutObject"),
      );
      expect(hasReadWrite).toBe(true);
      expect(hasReadOnly).toBe(true);
    });

    test("two prefixes with different OIDC providers: creates 2 provider resources, 2 roles", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["google-client"],
              },
            ],
          },
          {
            s3Prefix: "config/",
            oidcProviders: [
              {
                issuerUrl: "https://login.microsoftonline.com/tenant-id/v2.0",
                clientIds: ["azure-client"],
              },
            ],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // Two distinct OIDC providers
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 2);

      // Two federated roles
      const roles = template.findResources("AWS::IAM::Role");
      const federatedRoles = Object.values(roles).filter((r) =>
        r.Properties.AssumeRolePolicyDocument?.Statement?.some(
          (s: Record<string, unknown>) =>
            s.Action === "sts:AssumeRoleWithWebIdentity",
        ),
      );
      expect(federatedRoles.length).toBe(2);
    });

    test("throws when same issuerUrl has different clientIds across prefixes", () => {
      expect(() => {
        new ImmuKV(stack, "ImmuKV", {
          prefixes: [
            {
              s3Prefix: "pipeline/",
              oidcProviders: [
                {
                  issuerUrl: "https://accounts.google.com",
                  clientIds: ["client-a"],
                },
              ],
            },
            {
              s3Prefix: "config/",
              oidcProviders: [
                {
                  issuerUrl: "https://accounts.google.com",
                  clientIds: ["client-b"],
                },
              ],
            },
          ],
        });
      }).toThrow(
        'OIDC provider conflict: issuerUrl "https://accounts.google.com" is referenced by multiple prefixes with different clientIds',
      );
    });
  });

  describe("Multi-Prefix Integration", () => {
    test("complete multi-prefix setup produces correct resource counts", () => {
      new ImmuKV(stack, "ImmuKV", {
        prefixes: [
          {
            s3Prefix: "pipeline/",
            logVersionRetention: cdk.Duration.days(2555),
          },
          {
            s3Prefix: "config/",
            logVersionRetention: cdk.Duration.days(90),
            oidcProviders: [
              {
                issuerUrl: "https://accounts.google.com",
                clientIds: ["config-client"],
              },
            ],
          },
        ],
      });
      const template = Template.fromStack(stack);

      // 1 bucket
      template.resourceCountIs("AWS::S3::Bucket", 1);
      // 4 managed policies (2 per prefix)
      template.resourceCountIs("AWS::IAM::ManagedPolicy", 4);
      // 1 OIDC provider (only config/ has OIDC)
      template.resourceCountIs("Custom::AWSCDKOpenIdConnectProvider", 1);
    });

    test("prefix() returns distinct policies per prefix", () => {
      const immukv = new ImmuKV(stack, "ImmuKV", {
        prefixes: [{ s3Prefix: "pipeline/" }, { s3Prefix: "config/" }],
      });

      const pipeline = immukv.prefix("pipeline/");
      const config = immukv.prefix("config/");

      expect(pipeline.readWritePolicy).not.toBe(config.readWritePolicy);
      expect(pipeline.readOnlyPolicy).not.toBe(config.readOnlyPolicy);
    });
  });
});
