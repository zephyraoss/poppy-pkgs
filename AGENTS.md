# AGENTS.md

Guidance for agentic coding assistants working in `poppy-pkgs`.
This repository is a Go service that mirrors `winget-pkgs`, indexes manifests into SQLite, and serves a Fiber API.

## Source of truth

- Follow this file first for local agent behavior.
- No Cursor rules were found in `.cursor/rules/`.
- No `.cursorrules` file was found.
- No Copilot instructions were found in `.github/copilot-instructions.md`.
- If any of those files are added later, treat them as additional required constraints.

## Repository layout

- `cmd/poppypkgs/main.go`: application entrypoint and process lifecycle.
- `internal/api`: HTTP API handlers and response shaping.
- `internal/config`: environment-driven config loading.
- `internal/indexer`: manifest parsing and indexing orchestration.
- `internal/repo`: local git mirror management for `winget-pkgs`.
- `internal/store`: SQLite schema, queries, and search logic.
- `Taskfile.yml`: convenience tasks for fmt/lint/build.

## Prerequisites

- Go `1.25.x` (module declares `go 1.25.7`).
- Git installed and available in `PATH`.
- Optional: `task` CLI (`go-task`) for Taskfile commands.

## Build, run, lint, test

### Run locally

- `go run ./cmd/poppypkgs`

### Build

- `task build`
- Equivalent direct command:
- `mkdir -p testing && go build -o ./testing/poppypkgs.exe ./cmd/poppypkgs`

### Format

- `task fmt`
- Equivalent direct command:
- `gofmt -w ./cmd ./internal`

### Lint / static checks

- `task lint`
- Equivalent direct command:
- `go vet ./...`

### Full local CI-style pass

- `task all`
- Equivalent direct sequence:
- `gofmt -w ./cmd ./internal && go vet ./... && mkdir -p testing && go build -o ./testing/poppypkgs.exe ./cmd/poppypkgs`

### Test commands

Note: there are currently no `*_test.go` files in this repository.
Use these commands when adding or running tests.

- Run all tests:
- `go test ./...`
- Run tests in one package:
- `go test ./internal/store`
- Run one test function (exact name):
- `go test ./internal/store -run '^TestCompareVersion$'`
- Run one subtest:
- `go test ./internal/store -run '^TestCompareVersion/handles_prerelease$'`
- Run tests matching a pattern across all packages:
- `go test ./... -run 'CompareVersion|FTS'`
- Disable test caching for iterative debugging:
- `go test ./internal/store -run '^TestCompareVersion$' -count=1`
- Verbose output:
- `go test -v ./internal/store -run '^TestCompareVersion$'`

## Environment and runtime configuration

Supported environment variables (from `internal/config/config.go`):

- `POPPY_LISTEN_ADDR` (default `:8080`)
- `POPPY_DB_PATH` (default `./data/poppypkgs.db`)
- `POPPY_REPO_PATH` (default `./data/winget-pkgs`)
- `POPPY_REPO_URL` (default `https://github.com/microsoft/winget-pkgs.git`)
- `POPPY_SYNC_INTERVAL` (default `1m`; duration string or positive seconds)
- `POPPY_LOG_LEVEL` (`debug|info|warn|error`, default `info`)

## Code style guidelines

### Formatting and file hygiene

- Always run `gofmt` on changed Go files.
- Keep lines and blocks gofmt-friendly; avoid manual alignment.
- Keep package structure under `cmd/` and `internal/` consistent with existing layout.
- Do not introduce new generated files unless explicitly requested.

### Imports

- Use standard Go import grouping:
- First group: standard library.
- Second group: external and module imports.
- Let `gofmt` order imports lexicographically within each group.
- Prefer explicit imports; do not use dot imports.
- Keep blank identifier imports only when required (example: SQLite driver registration).

### Types and data modeling

- Prefer concrete structs for domain models (`Package`, `Version`, `ManifestFile`).
- Use `any` sparingly (currently used for YAML decoding and helper return types).
- Prefer zero-value-friendly structs and slices.
- Preserve JSON tags and API field naming conventions (`snake_case` JSON keys).
- Use strong type names with domain context (`ManifestRecord`, `Status`, `Config`).

### Naming conventions

- Exported identifiers use `CamelCase`; unexported use `camelCase`.
- Preserve common Go initialisms in all-caps: `ID`, `URL`, `DB`, `YAML`, `FTS`.
- Use descriptive receiver names (`d *DB`, `i *Indexer`, `c *Client`, `s *Server`).
- Keep function names action-oriented (`ListVersions`, `ApplyManifestChanges`, `RunOnce`).

### Error handling

- Return errors instead of panicking in library/internal packages.
- Use early returns for validation and failure paths.
- Use `errors.Is` for sentinel comparisons (`sql.ErrNoRows`, context cancellation, local sentinels).
- Wrap errors with context when crossing boundaries (see `repo.run`).
- In API handlers, log internal error details but return sanitized client messages.

### Context, concurrency, and cancellation

- Pass `context.Context` as the first argument for I/O or long-running operations.
- Honor cancellation checks in loops and walkers.
- Protect mutable shared state with mutexes (see `Indexer` status fields).
- Prefer bounded worker concurrency and channel-based fan-out/fan-in for batch parsing.

### Logging

- Use structured logging via `log/slog`.
- Message text should be short, lowercase, and action-oriented.
- Attach structured key-value pairs instead of formatting long strings.
- Log operational milestones and progress for long tasks (clone/fetch/scan/index).

### API and storage conventions

- Keep API routes versioned under `/api/v1`.
- Keep response shape consistent: usually `{ "data": ..., "meta": ... }` or `{ "error": ... }`.
- Use `fiber.Status*` constants for HTTP status codes.
- Keep SQL parameterized (`?`) and inside context-aware `QueryContext`/`ExecContext` calls.
- Prefer transactions for multi-step writes affecting relational consistency.

### Testing expectations for new work

- Add table-driven tests for pure logic helpers (for example, version comparison and tokenization).
- Add focused package tests around changed behavior before broad integration tests.
- For bug fixes, add at least one regression test that fails before the fix.
- Keep tests deterministic; avoid network and filesystem side effects unless required.
