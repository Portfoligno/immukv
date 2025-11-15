# Changelog

All notable changes to ImmuKV will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
