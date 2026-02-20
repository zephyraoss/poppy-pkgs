# poppy-pkgs

A Go service that mirrors `microsoft/winget-pkgs`, indexes manifests into SQLite (FTS5), and exposes a versioned API for package search and manifest download.

## Run

```bash
go run ./cmd/poppypkgs
```

## API (v1)


### Endpoints

- `GET /api/v1/health`
- `GET /api/v1/status`
- `GET /api/v1/search?q=<query>&limit=20&offset=0`
- `GET /api/v1/packages/:packageId`
- `GET /api/v1/packages/:packageId/versions`
- `GET /api/v1/packages/:packageId/versions/:version/manifests`
- `GET /api/v1/packages/:packageId/versions/:version/manifests/:manifestId/raw`
- `GET /api/v1/packages/:packageId/versions/:version/manifests/:manifestId/download`

`version` supports `latest` as a shortcut (for example: `/api/v1/packages/Foo.Bar/versions/latest/manifests`).

Search prioritization:
1. Exact `PackageIdentifier` match
2. Prefix `PackageIdentifier` match
3. FTS relevance

### Examples

Search packages:

```bash
curl "http://localhost:8080/api/v1/search?q=yucca&limit=5"
```

Get package metadata (includes `latest_version`):

```bash
curl "http://localhost:8080/api/v1/packages/Addi.Yucca"
```

List unique package versions:

```bash
curl "http://localhost:8080/api/v1/packages/Addi.Yucca/versions"
```

Fetch manifests for latest version:

```bash
curl "http://localhost:8080/api/v1/packages/Addi.Yucca/versions/latest/manifests"
```

Download a manifest file from API (no GitHub URL needed):

```bash
curl -L -o manifest.yaml "http://localhost:8080/api/v1/packages/Addi.Yucca/versions/latest/manifests/123/download"
```

### Optional Configuration (env vars)

- `POPPY_LISTEN_ADDR` (default `:8080`)
- `POPPY_DB_PATH` (default `./data/poppypkgs.db`)
- `POPPY_REPO_PATH` (default `./data/winget-pkgs`)
- `POPPY_REPO_URL` (default `https://github.com/microsoft/winget-pkgs.git`)
- `POPPY_SYNC_INTERVAL` (default `1m`, supports duration like `30s`)
- `POPPY_LOG_LEVEL` (`debug|info|warn|error`, default `info`)
