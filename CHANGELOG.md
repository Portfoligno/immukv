# Changelog

All notable changes to ImmuKV will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.22] - 2026-02-16

### Added

- CDK: OIDC federation support via `oidcProviders` prop and `federatedRole` property
  - Generalized OIDC provider configuration (Google, GitHub, etc.)
  - Automatic trust policy and IAM role creation
- Credential provider support for automatic credential refresh
  - TypeScript: `CredentialProvider` callback type (`() => Promise<StaticCredentials>`)
  - Python: `CredentialProvider` async callback type, backed by
    `AioDeferredRefreshableCredentials` for automatic refresh
  - Static credentials now use `StaticCredentials` (TypeScript) / `S3Credentials` (Python)
    with optional `sessionToken` / `aws_session_token` and `expiresAt` / `expires_at`
- `listKeysWithPrefix()` / `list_keys_with_prefix()` for server-side key filtering by prefix

### Changed

- Python: Switched S3 client from boto3 to aiobotocore for native async support

## [0.1.21] - 2026-02-11

### Fixed

- `verifyLogChain()` / `verify_log_chain()` no longer crashes when called from a
  `withCodec` client on a log containing entries from differently-typed clients
  - Verification now uses raw JSON values directly from S3, bypassing the
    decode/encode round-trip entirely
  - This is also strictly more correct for lossy codecs (no false negatives)
- Python: `log_entries()` and `history()` now send `KeyMarker` alongside
  `VersionIdMarker` in S3 `ListObjectVersions` calls, matching the TypeScript
  implementation and the S3 API contract

## [0.1.20] - 2026-02-11

### Fixed

- `withCodec()` / `with_codec()` clients no longer crash when the shared global log
  contains entries written by wider-typed or differently-typed clients
  - Internal operations (`getLatestAndRepair`, `repairOrphan`) now use raw log entries
    that bypass the value decoder entirely
  - Orphan repair writes the raw JSON value directly, eliminating the
    decode-then-encode round-trip that could lose fields
  - Orphan fallbacks in `get()` and `history()` decode on demand at the return site

### Changed

- CI: Version checks consolidated into a dedicated `version-check` job
  (includes `package-lock.json` verification for TypeScript and CDK)

## [0.1.19] - 2026-02-02

### Fixed

- Error thrown when `set()` exhausts retries now includes diagnostic details:
  - HTTP status code, error name/code, error message, and S3 request ID
  - Original S3 error attached as `cause` for full stack trace access

## [0.1.18] - 2025-11-21

### Added

- New `withCodec()` / `with_codec()` method to create client with different codec
  - Creates a new client instance sharing the underlying S3 connection
  - Allows working with different key/value types while reusing the connection pool
  - Derived client has independent mutable state (repair check, write permission, orphan status)
  - Note: Closing either client affects both due to shared S3 connection

## [0.1.17] - 2025-11-21

### Changed

- **BREAKING**: Client constructor now requires both `ValueDecoder` and `ValueEncoder` parameters
  - Python: `ImmuKVClient(config, value_decoder, value_encoder)`
  - TypeScript: `new ImmuKVClient(config, valueDecoder, valueEncoder)`
  - Renamed `ValueParser` to `ValueDecoder` for clarity
  - Added `ValueEncoder` for symmetric serialization
- **BREAKING**: TypeScript: All optional fields now use `| undefined` instead of `| null`
  - Affects method parameters: `history()`, `logEntries()`, `listKeys()`
  - Affects return types and Entry fields
  - Aligns with TypeScript idioms where `?` means `| undefined`
- Python: All client instance fields now private (prefixed with underscore)
  - `_config`, `_s3`, `_log_key`, `_value_decoder`, `_value_encoder`
- Internal: Reorganized package structure with `_internal/` modules
  - Moved factory functions and helper types to internal packages
  - Reduced public API surface to core types only

### Added

- Public API now exports all branded types for type annotations
  - `LogVersionId`, `KeyVersionId`, `KeyObjectETag`, `Hash`, `Sequence`, `TimestampMs`
  - `S3Credentials`, `S3Overrides` config helper types
- Cross-language JSON consistency
  - Python: Optional fields with `None` values are omitted from JSON
  - TypeScript: Optional fields with `undefined` values are omitted from JSON
  - Both languages produce identical JSON structure
  - Note: Only applies to log entry metadata fields (`previous_version_id`, `previous_key_object_etag`); the `value` field from `ValueEncoder` is stored as-is

### Fixed

- CI: Python boolean check workflow rewritten with clearer auto-coercion rule
  - Prevents false positives from `is True`, `== value`, and other explicit operators
  - Now correctly distinguishes auto-coercion from explicit boolean operators

## [0.1.16] - 2025-11-19

### Fixed

- **CRITICAL**: Python: Fixed bug where cached orphan entries could be incorrectly returned when they should not be
  - Affected `get()` and `history()` methods in read-only mode

### Changed

- Python: Internal code quality improvements for strict type checking compliance

## [0.1.15] - 2025-11-16

### Fixed

- Python: All implicit truthiness checks on Optional types replaced with explicit `is not None` checks
  - client.py: 20 violations fixed across config checks, pagination markers, and orphan status handling
  - json_helpers.py: 2 violations fixed in entry deserialization

### Changed

- CI: Python boolean check workflow now runs every 8 hours (previously 3x daily)

## [0.1.14] - 2025-11-15

### Added

- TypeScript: Strict boolean expression checking via ESLint rules
  - `@typescript-eslint/strict-boolean-expressions`: Require explicit comparisons instead of implicit truthiness
  - `@typescript-eslint/prefer-nullish-coalescing`: Use `??` instead of `||` for null coalescing
- Python: Explicit `is not None` checks instead of implicit truthiness for Optional types
- CI: Scheduled Claude Code Action workflow to automatically check Python code for implicit boolean expressions on Optional types (runs 3x daily)

### Changed

- Internal: Corrected AWS SDK type definitions to match actual API behavior
  - Both AWS SDK JS and boto3-stubs incorrectly mark field optionality
  - Added factory methods (`fromAwsSdk()`/`from_boto3()`) to reconstruct types at wrapper boundary
  - Changed optional field semantics from "field can be absent" to "field always present, value can be None/undefined"
- Internal: Updated all test references from LocalStack to MinIO

## [0.1.13] - 2025-11-14

### Added

- Python: Support for S3-compatible services via `overrides` field in Config
  - `S3Overrides` dataclass with `endpoint_url`, `credentials`, and `force_path_style` options
  - Enables usage with MinIO, LocalStack, and other S3-compatible services
- TypeScript: Support for S3-compatible services via `overrides` field in Config
  - Interface with `endpointUrl`, `credentials`, and `forcePathStyle` options
- Python: Comprehensive integration test suite using MinIO (9 tests)
  - Tests real S3 versioning, ETags, conditional writes, and object structure
- TypeScript: Comprehensive integration test suite using MinIO (9 tests)
- Python: Separate unit test suite (14 tests) for pure logic testing
- TypeScript: Separate unit test suite (14 tests) for pure logic testing
- CI: Integration test jobs in GitHub Actions using MinIO service containers

### Fixed

- TypeScript: `history()` method now returns correct results (previously returned empty due to incorrect pagination parameters)
- TypeScript: `logEntries()` method now returns correct results (previously returned empty due to incorrect pagination parameters)
- TypeScript: Log entry JSON serialization now uses `null` instead of `undefined` for `previous_key_object_etag` field

### Changed

- Python: Removed `moto[s3]` dependency in favor of real S3 integration tests
- TypeScript: Removed `aws-sdk-client-mock` dependency in favor of real S3 integration tests
- CI: Split tests into unit and integration categories
- CI: Integration tests now required to pass before publishing

## [0.1.12] - 2025-11-12

### Changed

- **BREAKING** (CDK): Refactored from Stack to Construct pattern
  - `ImmuKVStack` → `ImmuKV` (now extends `Construct` instead of `Stack`)
  - `ImmuKVStackProps` → `ImmuKVProps` (no longer extends `StackProps`)
  - Retention parameters now accept `cdk.Duration` instead of number
    - `logVersionRetentionDays` → `logVersionRetention` (Duration)
    - `keyVersionRetentionDays` → `keyVersionRetention` (Duration)
  - Default retention changed from limited to unlimited (preserves immutability)
  - Retention parameters are now fully independent (days OR count, not both required)
- Python: Internal refactoring - removed redundant type overloads in `BrandedS3Client`

### Added

- CDK: S3 event notifications support via `onLogEntryCreated` property
  - Supports Lambda functions, SNS topics, and SQS queues
- CDK: Comprehensive test suite with 44 tests using Jest
- CDK: Input validation for retention parameters and S3 prefix

### Fixed

- CDK: Stricter input validation (rejects path traversal, fractional days)

## [0.1.11] - 2025-11-11

### Added

- New `cdk-immukv` package: AWS CDK construct library for deploying ImmuKV infrastructure
  - TypeScript: Published to npm as `cdk-immukv`
  - Python: Published to PyPI as `cdk-immukv` (auto-generated from TypeScript via jsii)
  - Provides `ImmuKVStack` construct with configurable S3 bucket, lifecycle rules, and IAM policies

### Changed

- Python: Added `py.typed` marker for PEP 561 type checking support
- TypeScript: Enabled `declarationMap` for improved IDE navigation and debugging

## [0.1.10] - 2025-11-08

### Changed

- Internal: Refactored S3 wrapper code into organized module structure

## [0.1.9] - 2025-11-08

### Changed

- **BREAKING**: Python minimum version requirement increased to 3.11+ (previously 3.9+)
- **BREAKING**: Public API reduced to core interfaces only
  - Python: Removed hash_compute, hash_genesis, hash_from_json, sequence_initial, sequence_next, sequence_from_json, timestamp_now, timestamp_from_json from exports
  - TypeScript: Removed hashCompute, hashGenesis, hashFromJson, sequenceInitial, sequenceNext, sequenceFromJson, timestampNow, timestampFromJson, LogEntryForHash, OrphanStatus from exports
  - Public API now exposes: ImmuKVClient, Config, Entry, ValueParser, JSONValue, KeyNotFoundError, ReadOnlyError
- Internal: S3 path branding with generic key type parameter for improved type safety

## [0.1.8] - 2025-11-07

### Changed

- Internal: Factory functions for branded types with validation
- Internal: Enhanced Python boto3 integration with strict typing (TypedDict)
- Internal: Build-time version substitution for cleaner source code

## [0.1.7] - 2025-11-06

### Added

- Strong type safety with branded types for version IDs, hashes, and other S3-specific values
- Full generic type support allowing type-safe keys and values

### Changed

- **BREAKING**: ImmuKVClient constructor now requires a `ValueParser` parameter
  - Python: `ImmuKVClient(config, lambda v: v)`  # identity parser for no transformation
  - TypeScript: `new ImmuKVClient(config, v => v)`  # identity parser for no transformation
  - This enables type-safe value parsing and custom validation of stored values
- Improved type inference with generic K and V type parameters

### Fixed

- `list_keys` with `after_key` parameter now correctly excludes the specified key from results (previously included it)

## [0.1.6] - 2025-10-31

### Fixed

- Add repository field to package.json for npm provenance validation

## [0.1.5] - 2025-10-31

### Fixed

- CI: Install npm 11.5.1 for OIDC trusted publishing support

## [0.1.4] - 2025-10-31

### Fixed

- CI: Fix npm OIDC authentication by removing token-based auth setup

## [0.1.3] - 2025-10-31

### Fixed

- CI: Fix TypeScript version check to use Node.js instead of jq

## [0.1.2] - 2025-10-31

### Added

- Initial release of ImmuKV
- Core immutable key-value store using S3 versioning
- Two-phase write protocol (log-first, then key object)
- Automatic orphan repair with ETag-based conditional writes
- SHA-256 hash chain for cryptographic integrity
- Python client implementation with boto3
- TypeScript client implementation with AWS SDK v3
- Core operations: `set`, `get`, `history`, `log_entries`, `list_keys`
- Hash verification: `verify` and `verify_log_chain`
- Configurable repair check interval
- Read-only mode support
- Optional KMS encryption support

[0.1.22]: https://github.com/Portfoligno/immukv/releases/tag/0.1.22
[0.1.21]: https://github.com/Portfoligno/immukv/releases/tag/0.1.21
[0.1.20]: https://github.com/Portfoligno/immukv/releases/tag/0.1.20
[0.1.19]: https://github.com/Portfoligno/immukv/releases/tag/0.1.19
[0.1.18]: https://github.com/Portfoligno/immukv/releases/tag/0.1.18
[0.1.17]: https://github.com/Portfoligno/immukv/releases/tag/0.1.17
[0.1.16]: https://github.com/Portfoligno/immukv/releases/tag/0.1.16
[0.1.15]: https://github.com/Portfoligno/immukv/releases/tag/0.1.15
[0.1.14]: https://github.com/Portfoligno/immukv/releases/tag/0.1.14
[0.1.13]: https://github.com/Portfoligno/immukv/releases/tag/0.1.13
[0.1.12]: https://github.com/Portfoligno/immukv/releases/tag/0.1.12
[0.1.11]: https://github.com/Portfoligno/immukv/releases/tag/0.1.11
[0.1.10]: https://github.com/Portfoligno/immukv/releases/tag/0.1.10
[0.1.9]: https://github.com/Portfoligno/immukv/releases/tag/0.1.9
[0.1.8]: https://github.com/Portfoligno/immukv/releases/tag/0.1.8
[0.1.7]: https://github.com/Portfoligno/immukv/releases/tag/0.1.7
[0.1.6]: https://github.com/Portfoligno/immukv/releases/tag/0.1.6
[0.1.5]: https://github.com/Portfoligno/immukv/releases/tag/0.1.5
[0.1.4]: https://github.com/Portfoligno/immukv/releases/tag/0.1.4
[0.1.3]: https://github.com/Portfoligno/immukv/releases/tag/0.1.3
[0.1.2]: https://github.com/Portfoligno/immukv/releases/tag/0.1.2
