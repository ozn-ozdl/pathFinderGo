# Contributing

Thanks for your interest in contributing!

This project is **alpha** and currently has **Germany-specific assumptions** (see `README.md`).

## Development setup

### Backend (Go)

```bash
go version
go mod download

# Run with an OSM extract (.osm or .pbf)
go run ./src -osm /absolute/path/to/map.osm
```

The server can also be configured via environment variables:

- `OSM_FILE`
- `SERVER_ADDR`

If you use a local `.env` file, you can export it into your shell (example for bash/zsh):

```bash
set -a; source .env; set +a
go run ./src
```

## Repo hygiene

- Don’t commit large OSM extracts, caches, or build artifacts (see `.gitignore`).
- Prefer small, focused PRs with a short “why” in the description.

## API changes

If you change request/response shapes, please update:

- `README.md` endpoint docs
- Any sample `curl` snippets
- The legacy compatibility paths (if applicable)

