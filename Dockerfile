FROM golang:1.25-bookworm AS builder

WORKDIR /usr/local/src

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git make gcc libc6-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN go build -o ./bin/app ./cmd/

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/src/bin/app /app

CMD ["/app"]