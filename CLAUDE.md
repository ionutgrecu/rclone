# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build rclone (includes version info)
make

# Quick build without version info
go build

# Run all unit tests (uses RCLONE_CONFIG="/notfound" to avoid config conflicts)
make quicktest
# Or directly:
RCLONE_CONFIG="/notfound" go test ./...

# Run tests for a specific package
go test -v ./backend/drive

# Run tests with race detection
make racequicktest

# Run linting (requires golangci-lint)
make check
# Or directly:
golangci-lint run ./...

# Install golangci-lint
make build_dep

# Run integration tests against a backend (requires TestRemote config)
go test -v ./fs/operations -remote TestDrive:
go test -v ./fs/sync -remote TestDrive: -fast-list

# Run integration tests for a specific backend using test framework
go run ./fstest/test_all -backends drive
```

## Code Architecture

### Core Interfaces (fs/types.go, fs/features.go)

The `fs` package defines the fundamental interfaces that all storage backends must implement:

- **`Fs`**: Main filesystem interface with `List`, `NewObject`, `Put`, `Mkdir`, `Rmdir`
- **`Object`**: File interface with `Open`, `Update`, `Remove`, `SetModTime`
- **`Directory`**: Directory interface extending `DirEntry`
- **`Features`**: Optional capabilities struct - backends set function pointers for supported operations like `Purge`, `Copy`, `Move`, `DirMove`

### Backend Registration Pattern

Backends self-register via Go's `init()` mechanism:

1. Each backend in `backend/<name>/` calls `fs.Register()` in its `init()` function
2. `backend/all/all.go` imports all backends with blank imports (`_ "github.com/rclone/rclone/backend/..."`)
3. `rclone.go` imports `backend/all` to include all backends

The same pattern applies to commands in `cmd/`.

### Directory Structure

- **backend/** - Storage provider implementations (s3, drive, dropbox, etc.)
  - Each backend is a single file `<name>.go` plus optional `api/types.go` for API types
  - Virtual backends (alias, crypt, chunker, union) wrap other backends
- **cmd/** - CLI commands, each in its own subdirectory
- **fs/** - Core filesystem abstractions
  - `operations/` - High-level file operations (Copy, Move, etc.)
  - `sync/` - Directory synchronization logic
  - `config/` - Configuration management
  - `filter/` - Include/exclude filtering
  - `rc/` - Remote control API
- **vfs/** - Virtual filesystem layer for FUSE mounts and serve commands
- **lib/** - Shared utility libraries
  - `rest/` - HTTP REST client abstraction
  - `oauthutil/` - OAuth helpers
  - `pacer/` - Rate limiting with backoff
  - `dircache/` - Directory ID caching for backends
  - `encoder/` - Path encoding for different backends
- **fstest/** - Integration test framework
  - `fstests/` - Generic backend tests that run against any remote

### Writing a New Backend

1. Create `backend/<name>/<name>.go` (use `box` for directory-based or `b2` for bucket-based as templates)
2. Implement `Fs` and `Object` interfaces
3. Call `fs.Register()` in `init()` with backend metadata
4. Add import to `backend/all/all.go`
5. Create `backend/<name>/<name>_test.go` using fstests framework
6. For HTTP backends, use `lib/rest` and `fs/fshttp` for proper HTTP handling

### Commit Message Format

Prefix commits with the affected directory:
```
backend/drive: add team drive support - fixes #885
mount: fix hang on errored upload
```

### Testing Backends

Backend tests require a configured test remote named `Test<Backend>` (e.g., `TestDrive`):
```bash
# Unit tests (skipped if TestRemote not configured)
go test -v ./backend/drive

# Integration tests
go test -v ./fs/operations -remote TestDrive:
```
