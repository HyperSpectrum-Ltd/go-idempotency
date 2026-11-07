# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `ExecuteWithOptions` is now the single canonical execution entry for the
  executor (span, timeout, concurrency limit, retry, metrics).
  `ExecuteWithIdempotency*`, `ExecuteWithLock*`, `ExecuteWithTimeout`,
  `ExecuteParallel`, `ExecuteAsync` and `ExecuteWithCallback` remain as thin
  source-compatible wrappers and are marked deprecated with explicit
  replacements.
- `IdempotencyConfig` and `ExecutorConfig` share a `RuntimeConfig`
  (DefaultTTL / EnableMetrics / RetryDelay) instead of duplicating fields.
  Field access is unchanged via embedding; struct literals must use
  `RuntimeConfig: RuntimeConfig{...}` for those fields.
- `DistributedLockProvider` is documented as satisfying
  `go-domainkit/port.DistributedLock`; `IdempotencyConfig.BackoffPolicy()`
  exposes the unified retry policy.

### Added

- `ExecutionOptions`-based contract tests: canonical/legacy equivalence,
  service convenience delegation, shared `RuntimeConfig` identity.
