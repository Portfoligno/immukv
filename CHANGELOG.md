# Changelog

All notable changes to ImmuKV will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.7]: https://github.com/Portfoligno/immukv/releases/tag/0.1.7
[0.1.6]: https://github.com/Portfoligno/immukv/releases/tag/0.1.6
[0.1.5]: https://github.com/Portfoligno/immukv/releases/tag/0.1.5
[0.1.4]: https://github.com/Portfoligno/immukv/releases/tag/0.1.4
[0.1.3]: https://github.com/Portfoligno/immukv/releases/tag/0.1.3
[0.1.2]: https://github.com/Portfoligno/immukv/releases/tag/0.1.2
