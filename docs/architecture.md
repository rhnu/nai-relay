# Architecture

## Request ownership

The command loads TOML and environment settings once, opens a single SQLite connection, obtains a subscription snapshot and starts the HTTP service. A user is the zero-based position of an authenticated key in the configured array; the API exposes that same index. Keys and upstream credentials are never database identity fields.

The server owns the scheduler and serializes all quota mutations with one mutex. A failed durable write disables further dispatch. SQLite uses exclusive locking and DELETE journaling, so two instances cannot concurrently mutate the same records. The database parent directory is created on startup.

Each queued request owns its original body storage and response writer until the scheduler finishes it. Metadata extraction has a separate reader; it never reconstructs the outgoing JSON or multipart body. Large bodies spill to a temporary file. Temporary files are request-scoped and are not a persistent generation queue. A process crash can leave temporary files; they contain request content and operators should keep the temporary directory private.

## Resource accounting

All four resource pools use millionths of native units in signed 64-bit integers. Anlas conversion to official subscription fields rounds down to whole Anlas; the personal quota endpoint retains fractional values. Normal Opus has a configurable shared reserve and a per-user refill capacity. Actual bonuses above ordinary capacity remain available but are not increased by routine refill.

The state records user availability, reservations, physical resource estimates, upstream snapshots, estimated changes awaiting acknowledgment, and per-resource deficits. Repeated unchanged integer snapshots retain estimates rather than crediting back requests hidden by upstream rounding. Fractional recovery uses the upstream seconds-per-percentage-point rate within a bounded freshness window; a new snapshot acknowledges modeled changes without issuing them again. Startup obtains fresh evidence before estimating further recovery.

Recovery is distributed among users whose availability plus reservations is below capacity. Each newly full user leaves that distribution. Overflow at the global or individual capacity is discarded, never carried as a future entitlement. Purchased increments are divided among all fixed users. Deficits either proportionally reduce available balances or freeze the relevant resource until incoming recovery covers the shortfall.

Upstream spending order is authoritative: ordinary Opus before purchased Opus, and subscription Anlas before purchased Anlas. A request may combine pools only at the modeled global boundary and only with the caller's funds. With a positive regular Opus reserve, purchased Opus can remain inaccessible while the upstream ordinary pool still contains that reserve. Setting `reserve = 0` removes this deliberate restriction; the gateway does not silently exchange one user's purchased quota for another user's ordinary quota.

Reservations are persisted before dispatch. Sending is separately persisted before network I/O, so a crash between that write and transmission retains the charge under the `unknown` outcome. Explicit rejections refund; successful responses consume the reservation; responses without a confirmed outcome retain the estimate. Recent terminal outcomes are bounded persistent diagnostics, not an unlimited billing archive. No automatic retry replays a sent generation.

## Protocol boundary

Pricing reads the parameters needed to estimate native V3–V5 requests. It follows the maintained Rust binding's Anlas rules, with a separately configurable approximate V5 consumption multiplier. Unknown prices fail admission. Only source-image headers needed for tool pricing are decoded; image pixels are not transformed.

Native request encodings are JSON and multipart with a `request` metadata part. Cached image keys and secrets travel in those same native requests. No extra cache endpoints, proxy-side cache registrations, or secret substitutions are introduced.

Responses retain their original representation. SSE and four-byte big-endian-length-prefixed MessagePack have side observers for event types and sample indexes. A complete stream has one final image event per expected sample followed by normal EOF. Unknown events remain transferable. If inspection cannot establish completion, remaining bytes are still forwarded and the charge remains estimated. Nonstream bodies are relayed without unpacking archives or decoding image pixels.

Only the supported image routes are proxied. Subscription responses are reconstructed from safe fields and personal values; `/quota` never returns the global snapshot or other users' balances. Hop-by-hop headers and account credentials are not forwarded downstream.

## Scheduling and operation

Users alternate turns while each user's queue remains FIFO. Concurrency controls the size of finite dispatch batches. The server waits for a batch to finish before fetching a clean snapshot and starting another batch. Rate-limit responses establish account-wide cooldown without replaying the rejected generation. Quota query failures pause dispatch until a successful refresh.

Build and release workflows compile only Linux amd64 production artifacts. Tests cover pure state transitions, pricing, metadata parsing, stream observation and scheduling helpers with constructed inputs. The release workflow builds and publishes the container image. Database persistence, live HTTP transfer, container startup and actual upstream charges are outside the automated verification scope.
