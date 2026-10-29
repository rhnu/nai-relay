# nai-relay

A small Go gateway sharing one NovelAI image account among fixed, independently metered users. No Web UI. Linux amd64 binaries and container images are built by GitHub Actions.

## Run

Copy `config.example.toml` to `config.toml`, replace the upstream token and user keys, then run:

```sh
./nai-relay --config config.toml
```

Clients use the gateway URL and their assigned key as `Authorization: Bearer <key>`. All image generation for the upstream account must pass through this gateway. The upstream persistent token belongs only in the gateway configuration.

Every configuration field has an uppercase `NAI_RELAY_` environment override. Keys are a JSON array; environment keys replace the entire file array. Environment-only deployment needs no config file:

```sh
export NAI_RELAY_UPSTREAM_TOKEN='your-upstream-token'
export NAI_RELAY_KEYS='["first-user-key","second-user-key"]'
./nai-relay
```

Configuration is read at startup. Array position identifies the wallet: replacing a key at the same position preserves its balance; reordering keys changes which wallet each key accesses. Empty or duplicate keys fail validation. A changed user count is rejected. To change membership count, stop the service, delete its database and any SQLite sidecar files, and restart to split the current upstream balances again. This permanently discards the previous accounting records.

## Container

```sh
docker volume create nai-relay-data
docker run --rm -p 8080:8080 \
  --mount source=nai-relay-data,target=/data \
  -e NAI_RELAY_UPSTREAM_TOKEN \
  -e NAI_RELAY_KEYS \
  ghcr.io/rhnu/nai-relay:latest
```

The image runs as UID/GID 65532 and includes CA certificates. Bind-mounted data directories must be writable by this user. Only one gateway process may open a database. Place a TLS reverse proxy in front of the service for remote access, and allow sufficient request time for queueing and generation.

## API and transport

Supported native routes:

- `/ai/generate-image` and `/ai/generate-image-stream`
- `/ai/augment-image`, `/ai/encode-vibe`, `/ai/upscale`
- `/ai/generate-image/suggest-tags`
- `/user/subscription` (personal subscription projection)
- `/quota` (personal balances, capacity and state)
- `/healthz` and `/readyz`

JSON and multipart requests, cache references, ZIP/JSON/binary responses, SSE and length-prefixed MessagePack streams are supported. Cache keys and secrets are forwarded unchanged; clients handle upstream cache misses. The gateway preserves generation parameters and media bytes. Unsupported or unpriceable requests are rejected before sending; it is not an unrestricted proxy for account administration or text/voice APIs.

The request body is buffered before admission because multipart pricing metadata may follow image parts. Defaults: 8 MiB in memory, then a temporary file, with a 256 MiB request limit. Temporary request files are removed when their request ends. Prompt and image bodies are not saved in SQLite or logged.

## Quota behavior

Four resources are accounted separately: regular Opus, purchased Opus, subscription Anlas and purchased Anlas. One global regular Opus percentage point is reserved by default. With N users the ordinary refill capacity is `99/N`; the personal subscription meter displays 100 when this capacity is full.

Regular recovery is shared equally among wallets that are not full, counting reserved requests against capacity. Full wallets do not accumulate future recovery rights. Actual purchased additions are shared equally. Subscription Anlas replenishment fills personal capacities from actual upstream replenishment. Upstream priority pools must be spent first; a user cannot spend their purchased allowance by consuming another user's ordinary share.

Generation prices are estimates based on the maintained `novelai-bridge` rules and NovelAI's published V5 baseline. `usage_multiplier` scales estimated V5 consumption. Integer upstream percentage snapshots do not establish exact per-request charges. The default `deficit_policy = "proportional"` reduces balances proportionally on an uncovered deficit; `"freeze"` preserves balances and pauses the affected resources until covered.

The scheduler rotates among users, with FIFO within each user. Defaults are one upstream request at a time, a 10-second start interval, four waiting requests per user and a five-minute queue deadline. Insufficient funds return 402. Queue capacity and upstream rate limits return 429. Upstream requests are not automatically replayed.

Requests are reserved before sending. Requests confirmed as unexecuted are refunded. A sent request that disconnects, times out or is interrupted by a process exit retains its estimated charge and is recorded as `unknown`. Refunds require confirmation that the upstream did not execute the request. A complete stream requires a final image event for every expected sample followed by normal EOF.

## Build and release

Go 1.27 or later is required; automatic Go toolchain selection can download it when needed.

```sh
go vet ./...
go test ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath ./cmd/nai-relay
docker build -t nai-relay .
```

CI checks formatting, runs `go vet` and pure logic unit tests, and builds the Linux amd64 executable. Tests use constructed inputs without a live NovelAI account, HTTP test server or database integration suite. Version tags publish a Linux amd64 archive, SHA-256 checksums and a GHCR image; [v0.1.0](https://github.com/rhnu/nai-relay/releases/tag/v0.1.0) provides these artifacts. Stable releases update `latest`; prerelease tags do not. GitHub Actions requires repository contents/package write permissions for releases. No upstream token is needed in CI.

CI verifies builds and publication; live NovelAI operation and actual upstream charges are outside its verification scope. The generation-integrated `parameters.upscale` option is rejected; use the separate native `/ai/upscale` operation. See [architecture](docs/architecture.md) for accounting and protocol boundaries.

Licensed under Apache-2.0. Not affiliated with NovelAI. Pricing and protocol references include the author's [novelai-bridge](https://github.com/rhnu/novelai-bridge), the [official schema](https://image.novelai.net/docs/doc.json), and [Opus usage explanation](https://journal.novelai.net/opus-usage-limit-explained/).
