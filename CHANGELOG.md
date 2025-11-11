# Changelog

All notable changes to ImmuKV will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
