FROM golang:1.23-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY ./src ./src
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/pathfindergo ./src

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/pathfindergo /app/pathfindergo

EXPOSE 8080

# Pass your own args at runtime, e.g.:
# docker run --rm -p 8080:8080 -v "$PWD/data:/data" <image> \
#   -addr :8080 -osm /data/map.osm
ENTRYPOINT ["/app/pathfindergo"]
