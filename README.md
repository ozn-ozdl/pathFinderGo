# pathFinderGo

Explore railway infrastructure from OpenStreetMap extracts and compute plausible rail paths between stops.

This is an **early-stage (alpha)** project.

## Scope (Germany-only for now)

Current stop/identifier assumptions are **Germany-specific**:

- This code is currently intended to be used with **Germany GTFS datasets** such as **DELFI GTFS** and **ZHV**.
- `ifopt` in API requests is expected to refer to the station identifier used by those datasets:
  - **ZHV**: `ifopt` = **DHID / station id**
  - **DELFI GTFS**: `ifopt` = **station id**

If you use data outside of this ecosystem/country, you will likely need to adapt the stop-resolution logic and indexing.

This repo ships a small Go HTTP server that:

- Loads an OSM extract (`.osm` or `.pbf`)
- Extracts railway stations + railway ways
- Builds a route graph (cached on disk)
- Exposes a simple HTTP API

## Quickstart

```bash
go mod download

# Run with an OSM extract (.osm or .pbf)
go run ./src -osm /absolute/path/to/map.osm

# Custom listen address (default: :8080)
go run ./src -addr :9090 -osm /absolute/path/to/map.osm
```

Using `.env` (optional):

```bash
cp .env.example .env
# then export vars (example for bash/zsh)
set -a; source .env; set +a
go run ./src
```

Using Docker:

```bash
docker build -t pathfindergo .
docker run --rm -p 8080:8080 -v "$PWD/data:/data" pathfindergo \
  -addr :8080 -osm /data/map.osm
```

Environment variables are supported for convenience:

- `OSM_FILE`: same as `-osm`
- `SERVER_ADDR`: same as `-addr`

If neither `-osm` nor `OSM_FILE` is set, the server looks for `./data/map.osm`.

## Caching

On first startup, the server parses the OSM file and writes caches next to the OSM file:

- `.cache/railwaydata-<signature>.gob` (stations + rail lines)
- `.cache/railgraph-<signature>.gob` (route graph)

The cache key includes file path + size + mtime + a schema version, so caches auto-invalidate when the input changes.

## HTTP API

Preferred, stable-ish routes are under `/api/v1/*`. Legacy compatibility routes under `/railway/*` remain available.

### Health

- `GET /api/v1/health`
- `GET /health` (legacy)

### Data exploration

- `GET /api/v1/stations` (legacy: `GET /railway/stations`)
- `GET /api/v1/lines` (legacy: `GET /railway/lines`)

### Routing (v1)

`POST /api/v1/route`

Body:

```json
{
  "from": { "station_id": 12345 },
  "to":   { "lat": 48.8583, "lon": 2.2945 },
  "k": 3,
  "algo": "astar",
  "use_accel_decel": true,
  "corridor_similarity_threshold": 0.82
}
```

Each stop can be specified by **one** of:

- `ifopt` (string)
- `station_id` (number or string)
- `lat` + `lon`

Example:

```bash
curl -sS localhost:8080/api/v1/route \
  -H 'content-type: application/json' \
  -d '{"from":{"lat":48.8583,"lon":2.2945},"to":{"lat":48.8606,"lon":2.3376},"k":3,"algo":"astar"}'
```

### Routing (legacy)

`GET /railway/route?fromStationID=...&toStationID=...`

Also supports coordinates:

`GET /railway/route?fromLat=...&fromLon=...&toLat=...&toLon=...`

### Multi-segment trip paths (v1)

`POST /api/v1/trip/paths`

Body:

```json
{
  "stops": [
    { "ifopt": "de:123:456" },
    { "ifopt": "de:123:789" }
  ],
  "k": 3,
  "algo": "astar",
  "use_accel_decel": true
}
```

Legacy endpoint: `POST /railway/trip/paths` (supports older request shapes for backward compatibility).

