# Changelog

All notable changes to this project will be documented in this file.

## [2.1.10-9]

### Added
- Replaced `dropwizard-hibernate` with direct use of `hibernate-core`.

  **Reason**: `dropwizard-hibernate` is a Dropwizard bundle.
  - With `-Db.shards` deprecated in favor of multi-tenancy, shard configuration is only available during `run()`. This meant Hibernate bundles couldn't be initialized in `initialize()`, violating Dropwizard lifecycle expectations. 
  - Session factories couldn’t be initialized in parallel, as bundles must be added to the Dropwizard environment, which isn't thread-safe (`List<LifeCycle>` is not concurrent).
  
- Added support for parallel `SessionFactory` initialization per tenant.
- Introduced `BucketObserver` support:
  - Bucket IDs are now automatically populated in the column annotated with `@BucketKey`.
  - Population is based on `@LookupKey` in `LookupDao` or `@ShardingKey` in `RelationalDao`.

### Changed
- `skipNativeHealthCheck` is now part of `ShardingBundleOptions` (moved from `BlacklistConfig`).
- Defaulted `skipNativeHealthCheck` to `true`, meaning native health checks are skipped by default.

### Deprecated
- `BlacklistConfig` has been removed.

### Notes
- To enable the blacklisting feature:
  - Provide a concrete implementation of `ShardBlacklistingStore` during bundle initialization.
  - By default, a `NoopShardBlacklistingStore` is used.
- If blacklisting is enabled, native health checks will be skipped automatically, regardless of the `skipNativeHealthCheck` option.
- Important: Skipping health checks means the application will report as healthy even if one or all database shards are down.