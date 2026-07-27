# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo).

Paseo Relay keeps its public WebSocket protocol independent from its deployment platform. Nodes use OTP only for discovery and route ownership. A deployment adapter reroutes WebSocket upgrades to the owning node, so frames stay inside one BEAM node.

This project is under active development and its internal protocols may change without notice.

## Protocol compatibility

`serverId` and v2 `connectionId` are accepted only up to 256 bytes. This is a
deliberate compatibility difference from the Durable Object relay, which does
not impose this relay-side route-key limit. Oversized identifiers receive `400`
before they can create ownership or session state.

## Development

The Elixir and Erlang versions are managed with [asdf](https://asdf-vm.com/):

```sh
asdf install
mix deps.get
mix test
scripts/verify-patched-dependencies.sh
```

The dependency verifier fetches pinned pristine Bandit and Thousand Island
artifacts, verifies their hashes, compares the declared local patches, and runs
both upstream suites. Pass `--include-slow` to include external Docker/Autobahn
checks.

See [LICENSE](LICENSE).

## Operations

The release has no deployment-provider dependency. Its settings are intentionally
generic:

| Setting | Default | Meaning |
| --- | --- | --- |
| `PASEO_RELAY_HOST` | `127.0.0.1` | Public listener IP. |
| `PASEO_RELAY_PORT` | `4000` | Public HTTP/WebSocket listener. |
| `PASEO_RELAY_DRAIN` | `false` | Start not-ready while existing sessions drain. |
| `PASEO_RELAY_OWNERSHIP_TARGET` | `local` | Opaque target advertised to other relay nodes. |
| `PASEO_RELAY_REROUTE_HEADER` | `x-reroute-target` | Response header used by the deployment adapter. |
| `PASEO_RELAY_CLUSTER_QUERY` | unset | Optional DNS query used to discover BEAM peers. |
| `PASEO_RELAY_MIN_CLUSTER_SIZE` | `1` | Minimum nodes required before accepting unowned sessions. |
| `PASEO_RELAY_ACCEPTORS` | `100` | Listener acceptor processes. |
| `PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR` | `200` | Live connections allowed per acceptor; the default node ceiling is 20,000. |
| `PASEO_RELAY_CONNECTION_RETRY_COUNT` | `5` | Bounded listener retries when an acceptor reaches its connection ceiling. |
| `PASEO_RELAY_CONNECTION_RETRY_WAIT_MS` | `1000` | Delay between bounded listener retries. |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | Node-wide weighted WebSocket ingress ceiling. Must admit one maximum assembled message at the configured weight. |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | Conservative memory weight charged per wire payload byte. |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | Maximum reservation/write-barrier wait before a slow destination is shed. Also configures the TCP send timeout. |
| `PASEO_RELAY_PAYLOAD_TIMEOUT_MS` | `30000` | Maximum time from first admitted data-frame payload through completion of its assembled WebSocket message before retryable `1013` closure. Interleaved control frames do not reset it. |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | Maximum time a v2 client frame waits for its daemon-data socket. |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | Per-socket receive buffer limiting bytes accepted before header admission. |
| `PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS` | `33554432` | Per-WebSocket BEAM heap fuse, including shared binaries. Values below this protocol-safe floor are rejected. |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | Optional BEAM total-memory watermark that closes the oldest backpressured source with `1013`; disabled generically because the safe threshold depends on the deployment memory limit. |
| `RELEASE_NODE` / `RELEASE_COOKIE` | unset | Standard distributed-release identity. |

`GET /health` is a liveness probe. `GET /ready` returns `200` only while the
node accepts new work, and returns `503 {"status":"unready"}` while draining
or below the configured cluster floor. `GET /metrics` is Prometheus text and
exposes readiness, draining, active WebSockets, active sessions, reroutes,
listener rejections, delivery pressure and latency, frame sizes, slow-consumer
closes, ingress reservations, and BEAM memory for the local node.

Payloads never pass through a node-wide relay mailbox. Each `serverId` Owner
stores topology metadata only, and every destination has a Writer allowing one
payload write at a time. During delivery Bandit suspends source reads but keeps
the WebSocket process available for full-duplex writes; it rearms the source
only after the destination's TCP send barrier. Kernel TCP pressure therefore
reaches the producer without deadlocking simultaneous opposite-direction
traffic. Header-time weighted admission bounds simultaneous frame extraction
before payload bytes are read. An admitted data message, including every
fragment in a fragmented message, must complete within one configured payload
deadline, so header-only or non-final-fragment peers cannot retain weighted
budget indefinitely. The protocol preserves its existing 32 MiB wire-frame
ceiling (therefore 32 MiB minus the 14-byte client header for one maximum
unfragmented payload) and separately allows an assembled fragmented-message
payload of exactly 32 MiB.

See [`OPERATIONS.md`](OPERATIONS.md) for the production failure model,
capacity policy, and alerting signals.

Build a production release with `MIX_ENV=prod asdf exec mix release`, or build
the generic container with `docker build -t paseo-relay .`. The explicit
provider adapter in [`deployment/fly`](deployment/fly) translates its platform
node input into `RELEASE_NODE`; nothing under `lib/` or `scripts/` depends on it.

Cluster ownership uses [Syn](https://hexdocs.pm/syn/readme.html), an eventually
consistent distributed process registry. A network partition can temporarily
admit one owner for the same `serverId` on each side. When the registry
converges, Syn keeps one owner and the losing owner's WebSockets close with
`1012`, causing clients to reconnect through normal routing. Existing
WebSockets have no transparent migration or cross-partition forwarding. Restore
or fence a prolonged partition before treating reconnects as one session again;
see [`OPERATIONS.md`](OPERATIONS.md) for the operational consequence.

## Black-box load testing

The executable client uses actual WebSockets and the deployed v2 query contract:
`serverId`, `role`, optional `connectionId`, and `v=2`. It opens matching
server-data and client roles, so it can measure bidirectional traffic without
importing relay code. It prints one JSON object containing connection success and
failure counts, requested pairs and WebSockets, setup and steady durations, frame
throughput, p50/p95/p99 latency, duration, cleanup timeouts, client RSS/CPU,
and optional relay RSS/CPU (`--relay-pid`).

Safe local smoke test:

```sh
node scripts/relay-load.mjs --scenario idle --pairs 10 --duration 10
node scripts/relay-load.mjs --scenario sustained --pairs 10 --rate 10 --duration 10
node scripts/relay-load.mjs --scenario ownership --servers 1000 --batch-size 200 --duration 1
```

Distributed ownership and reroute decisions are exercised with real local BEAM
peer nodes in the test suite:

```sh
mix test test/paseo_relay/router_integration_test.exs test/paseo_relay_test.exs
PASEO_OWNERSHIP_SURGE_COUNT=50000 mix test test/paseo_relay_test.exs
```

A multi-node data test must run behind a deployment adapter capable of replaying
the original WebSocket upgrade. The Fly adapter uses `fly-replay`; all load
clients still use one public endpoint and the proxy performs node placement.

Capacity tests need an appropriate file-descriptor limit and kernel socket
budget. The ownership scenario opens one real daemon-control WebSocket for each
distinct `serverId`; it measures ownership churn rather than many clients on one
daemon. Example high-load commands, deliberately not defaults:

```sh
ulimit -n 120000
node scripts/relay-load.mjs --scenario ownership --servers 15000 --batch-size 500 --duration 30 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario idle --pairs 25000 --batch-size 250 --ramp-ms 100 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario sustained --pairs 1000 --batch-size 250 --ramp-ms 100 --rate 5 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario reconnect --pairs 1000 --batch-size 250 --ramp-ms 100 --reconnects 20 --duration 10
```

The sustained example sends in both directions: 1,000 pairs × 5 ticks/s × 2
frames = 10,000 frames/s. The 50,000-socket command is a stress target for a
node configured with a higher ceiling and enough memory, not the production
default. `--batch-size` bounds concurrent opens; `--ramp-ms` spaces each
batch. `--duration` measures steady traffic only, while JSON reports setup and
steady durations separately. Teardown waits up to 15 seconds for clean close
handshakes by default; use `--cleanup-grace` to tune that bound. A close that
does not finish within the bound is reported as `cleanup_timeouts` and still
makes the run fail. Abnormal closes remain connection failures.

To distribute a run across load generators, open the daemon control socket on
one generator and pass `--no-control` to the others. Give every generator a
different `--connection-prefix`; they can then share one `--server-id` without
connection ID collisions, exercising a single relay owner through reroutes.
Use `--keepalive` when a load scenario should include application-level
heartbeat traffic. The relay itself does not impose an idle WebSocket timeout.

A disposable load-generator image is available without any deployment-provider
assumptions:

```sh
docker build -f deployment/load/Dockerfile -t paseo-relay-load .
```
