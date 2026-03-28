# Contributing to FileStorage

## Scope and Architecture

- FileStorage is a .NET 9 embedded storage library.
- The public API is method-based (CRUD/query/index operations), not SQL-based.
- Keep changes minimal and targeted; avoid unrelated refactors.

## Project Constraints (Intentional)

- Payload model is single-field string data at table API level.
- No SQL parser or query language is in scope.
- No relational constraints (foreign keys, unique/check constraints, etc.) are planned.
- No multi-operation transaction model (`BEGIN/COMMIT/ROLLBACK`, isolation levels) is planned.

## Durability and Recovery Model

- Durability is provided by WAL + checkpoint/recovery mechanics.
- Batch save behavior should remain WAL-first and recovery-safe.
- Do not introduce transaction abstractions unless explicitly requested by maintainers.

## Contribution Rules

- Preserve architecture boundaries:
  - `FileStorage.Abstractions`: contracts only.
  - `FileStorage.Application`: API orchestration.
  - `FileStorage.Infrastructure`: engine internals.
- Do not add helper or behavioral logic (e.g., encoding helpers, convenience extensions) to `FileStorage.Abstractions`; keep such logic in consuming layers/samples.
- Keep public API and samples in sync when signatures change.
- Preserve existing async/cancellation and naming/style patterns.

## Repository Discoverability (Required)

- Keep a canonical path map up to date in `.github/copilot-instructions.md` and `docs/architecture.md`.
- When moving files or changing namespaces, update all affected path references in the same change.
- Maintain path aliases for known moved files (old path -> new path) in docs to reduce navigation friction.
- Prefer namespace-folder alignment for infrastructure internals to improve search/navigation consistency.
- Keep key entry files explicitly listed: provider entry point, table behavior, WAL/recovery/checkpoint, DI extensions, tests, samples.

## Validation Before PR

- Build succeeds for the full solution.
- Updated docs/samples if public behavior changed.
- Recovery and persistence assumptions remain valid (WAL/checkpoint/replay).
- Path references in docs and guidance files resolve to existing files