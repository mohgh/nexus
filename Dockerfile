# Multi-stage build for Nexus binaries.
# Produces a minimal (~20MB) scratch-based image with no shell or OS.
#
# Build:  docker build -t nexus .
# Run:    docker run -p 8000:8000 --env-file .env nexus
#
# The same Dockerfile builds all three binaries. Select which to run
# via the CMD override:
#   docker run nexus ./batch-aggregator
#   docker run nexus ./stream-processor

# ─── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server            ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/batch-aggregator   ./cmd/batch-aggregator
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/stream-processor   ./cmd/stream-processor

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM alpine:3.20

# ca-certificates for TLS connections to external services.
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/ .

EXPOSE 8000

# Default: run the HTTP server.
CMD ["./server"]
