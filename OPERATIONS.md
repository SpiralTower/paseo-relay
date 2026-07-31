# Operating Paseo Relay

## The bar

The relay is critical infrastructure. People run their entire working day
through it — agents, terminals, mobile sessions — and a blip of even a few
seconds is user-visible. A stuck node is a broken workday for everyone whose
sessions it owns. Treat every production action — deploys, restarts,
diagnostics, config changes — as something users will feel, and never stack
two of them (a diagnostic during a reconnect surge, a deploy during a
migration) without deciding that the combination is safe. When in doubt,
don't touch production.

## Diagnostics discipline

Every recurring observability question must be answerable from `/metrics`,
logs, or a new low-cardinality metric — never by interrogating live
processes.

- **Never call `:sys.get_state/1` (or any full-state dump) on a production
  Owner or singleton process.** Copying a large state term blocks that process.
  The retired node-wide Registry was previously wedged this way during a
  reconnect surge. Payloads now bypass global coordination, but full-state
  diagnostics remain unsafe and unnecessary.
- Targeted RPC reads during an incident are acceptable: `:syn.lookup/2` for a
  single key, one counter, one small map. Full-table or full-state scans are
  not, and the busiest node during a surge is the worst possible target.
- If a question keeps coming up (sessions by role, ownership counts, orphan
  sessions), add a gauge to `PaseoRelay.Metrics` and read it from `/metrics`
  like everything else.

## Capacity model

The relay does not assume a particular production topology. An operator may
run one Machine, several regional Machines, or provision stopped spares. The
Fly template disables automatic Machine starts so capacity changes remain an
explicit operator action. Fly never creates Machines automatically.

The template sets a 10,000-connection soft limit and a 15,000-connection hard
limit per Machine. The soft limit is a Fly placement signal, not a relay
failure threshold. Crossing it alone says nothing about application health;
with automatic starts disabled, it does not start a spare. At the hard limit,
Fly stops assigning new connections to that Machine. Existing connections
remain open. Any increase in `paseo_relay_connection_rejections_total` is a
separate application-level signal and should be treated as real user impact.

Stopped Machines still participate in deployment management: a Machine lease
or state transition can block `fly deploy` even though it is not serving
traffic. A stopped Machine that must remain unavailable to Fly Replay should
also be cordoned; stopping and disabling autostart are not routing fences.

Relay sessions are intentionally not rebalanced. A `serverId` is owned by one
BEAM node so its frames never cross nodes. Additional Machines increase total
fleet capacity for new sessions; they cannot split one exceptionally large
session across Machines.

The relay has a second, provider-independent safety ceiling. The application
multiplies `PASEO_RELAY_ACCEPTORS` by
`PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR`, yielding 20,000 active WebSockets per
node by default. Every valid local upgrade reserves one node-local slot before
Cowboy takes over the WebSocket. At the ceiling, the upgrade receives `503`,
`paseo_relay_connection_rejections_total` increments, and existing WebSockets
remain open. Slots are released explicitly at normal termination and by process
monitoring after abnormal death.

Ranch's own connection accounting is not this safety boundary: Cowboy removes
an upgraded connection from Ranch accounting at WebSocket takeover. Ranch
limits only concurrent pre-upgrade HTTP handling and applies TCP backlog
pressure there. Pre-upgrade HTTP parsing and unread request bodies have a
15-second idle deadline; upgraded WebSockets keep their protocol-level infinite
idle lifetime. The relay adds no application queue for over-capacity upgrades;
an explicit retryable rejection is safer than retaining another socket and
hiding overload as a timeout.

The native-Cowboy capacity run held 15,000 distinct real WebSockets with zero
failures and measured 1,300,512,768 bytes peak relay RSS on the test host. That
is the measured real-socket envelope; the generic 20,000-slot ceiling is a
final admission fuse, not evidence that 20,000 sockets fit every 2 GB runtime.
Provider limits and machine sizing must stay inside a locally measured envelope
with room for the configured ingress budget and VM overhead.

## Failure behavior

- **Relay process or Machine exits:** WebSockets on that owner disconnect. OTP
  removes the dead owner, clients reconnect, and a surviving node can claim the
  session. There is no transparent connection migration.
- **One region is unavailable:** Fly routes reconnects to a healthy region. The
  extra round trip is temporary; the replacement owner is then stable.
- **A node crosses the soft limit:** this is capacity information, not an
  incident. Existing sessions remain on their owners. If the deployment has
  automatic starts enabled, Fly may start an existing stopped Machine; the
  repository's Fly template keeps automatic starts disabled.
- **A node reaches the hard limit:** Fly stops sending it new connections.
  Existing connections remain open. A client pinned to that owner may fail to
  reconnect until capacity is available on the owner or the owner disappears.
- **A rolling deployment replaces an owner:** its WebSockets reconnect just as
  they would after a Machine exit. Ownership convergence can make one logical
  session reconnect more than once during a rollout; there is no single-reconnect
  guarantee. Drain is process-local admission state, initialized at boot with
  `PASEO_RELAY_DRAIN` or changed inside the application through
  `PaseoRelay.Drain`; there is no drain HTTP endpoint. Fly's deployment
  lifecycle does not activate this state, so it does not protect ownership
  during `fly deploy`.
- **A node wedges but stays clustered — the worst failure mode.** If a node
  stops answering HTTP (health check critical, `/metrics` unresponsive) while
  its BEAM stays connected to the cluster, Syn keeps its ownership
  registrations alive: reconnecting daemons are rerouted into the wedged node
  and cannot re-home elsewhere. Every session it owned is held hostage until
  the node dies. Remedy: restart the Machine promptly — the restart drops the
  node from the cluster, purges its registrations, and stranded sessions
  re-claim on healthy nodes within seconds. Do not wait for a wedged node to
  recover on its own, and do not diagnose it with anything heavier than its
  logs. Planned follow-up: an in-VM watchdog that self-terminates the node
  when its own readiness or Owner call latency degrades, so this recovery
  does not require an operator.
- **Fly Proxy loses the path to a locally healthy Machine:** forced-instance
  requests time out even though loopback HTTP, Owner routing, CPU, and memory
  remain healthy. New clients cannot reach sessions owned by that Machine.
  Start and verify the stopped spare in the same region before disturbing the
  affected Machine, then restart the affected Machine once. If the failure
  returns after a restart, replace the Machine on a fresh Fly host instead of
  repeatedly restarting it. This is a platform ingress failure, not relay
  pressure, and changing relay size will not repair it.
- **A destination stops reading:** its Writer permits only one payload write.
  Source reads remain suspended while its WebSocket process can still service
  full-duplex writes. The Writer records slow-consumer shedding at its delivery
  deadline, before the longer TCP send deadline, while healthy fanout
  destinations continue. A peer that resumes reading in time can receive the
  queued `1013`; a peer whose TCP receive path remains completely blocked can
  only observe transport closure because no WebSocket close frame can traverse
  that blocked path. Increasing `paseo_relay_backpressured_sources` is expected
  during brief congestion; sustained growth plus delivery timeouts or
  slow-consumer closes is actionable.
- **A control destination stops reading:** all control notifications use that
  destination's Writer. Once its bounded control queue fills, the control socket
  receives retryable `1013`; an accepted queued notification that reaches its
  absolute deadline also closes the destination instead of disappearing.
  Notifications cannot accumulate in an unbounded WebSocket mailbox.
- **Ingress reaches the node budget:** every complete message Cowboy delivers
  receives a token before protocol dispatch. Frames already buffered when source
  reads are suspended are separately tokenized and retained in source order. One
  monitored capacity ledger owns connection slots, weighted reservations, active
  deliveries, pressure order, and all related gauges. Socket death atomically
  drops every token, including after a forced heap-fuse exit. If the ledger dies,
  the runtime supervisor stops the listener and every old connection before
  restarting the ledger and reopening admission; this intentionally creates a
  node-local reconnect wave rather than overlapping accounting epochs. If the
  weighted total would cross the configured ceiling, that source closes with
  retryable `1013`; `paseo_relay_ingress_reserved_bytes` never exceeds the
  ceiling.

  Known remote owners are rerouted without consuming local capacity. For local
  or unowned sessions, connection admission is acquired before reserving or
  creating an Owner, so a capacity or pressure rejection cannot leave transient
  session state behind. Pre-upgrade HTTP parsing and unread request bodies have
  a finite idle lifetime; upgraded WebSockets explicitly retain an infinite
  protocol idle timeout and rely on their application lifecycle instead.

  The masked client data-frame wire ceiling remains exactly 32 MiB, so Cowboy
  bounds each complete or reassembled fragmented message to `32 MiB - 14 bytes`
  and sends `1009` for one byte more. Inbound v2 control input is limited to
  64 KiB, charged through JSON parsing, and oversized input receives `1009`.
  Incomplete fragments have not crossed the application boundary, so Cowboy's
  message ceiling, the per-WebSocket heap fuse, and the node memory watermark
  protect them instead. A pressure episode pauses new connection and message
  admission, sheds the oldest blocked delivery first and then the newest active
  sockets as a conservative proxy for recent fragment retention, and sizes
  subsequent bounded batches from measured BEAM memory relief. Admission
  resumes only below a one-maximum-message hysteresis threshold. The generic
  watermark is disabled because the safe threshold depends on the runtime
  limit; a strict generic deployment must set a nonzero threshold. The 2 GB Fly
  template enables it at 1.5 GB.
- **Two nodes concurrently claim a previously unowned `serverId`:** Syn favors
  availability, so both WebSockets can initially open against different local
  owners. Conflict resolution keeps one owner and closes sockets on the loser
  with `1012 Session owner moved`. Clients must reconnect and route to the
  winner. This is most visible during simultaneous daemon/client startup or a
  reconnect wave after ownership expires. A test that waits only for a control
  message and ignores the `1012` close will report a misleading timeout.
- **An upstream proxy sits in front of the relay (e.g. during a migration):**
  the proxy has its own connection lifecycle. Its deploys can sever a large
  and unpredictable fraction of relay connections at once, and its code
  propagation windows can strand clients on the old path. Treat any upstream
  deploy as a fleet reconnect storm: schedule it deliberately, never stack it
  with other production actions, and verify session convergence afterward.
- **The BEAM cluster partitions:** ownership uses Syn's available, strongly
  eventually consistent registry rather than a quorum or shared store. Each side
  can admit an owner for the same `serverId` while disconnected. When the
  cluster heals, Syn resolves the conflict to one owner; sockets monitoring a
  losing owner close with `1012` and reconnect. Existing WebSockets cannot
  forward or migrate across the partition. Restore connectivity or fence one
  side before treating reconnects as converged; do not advertise this topology
  as lossless failover.

## Incident response guardrails

Protect connected users before restoring the preferred topology.

- Confirm a failure with two independent signals or repeated probes. An OOM,
  Machine exit, failed health check, or unreachable forced-instance endpoint is
  already a concrete signal; CPU steal alone is not.
- Never restart more than one Machine at a time. Never restart the whole
  cluster. Existing sockets on the restarted Machine will disconnect.
- Before restarting an unhealthy Machine, start a stopped spare in its region
  when one is available and wait for `/ready` to pass. Do not stop any Machine
  that has acquired sessions during an incident merely to restore the preferred
  topology.
- Restart a Machine when it is wedged or unreachable, not merely busy. Afterward,
  verify the Machine's forced-instance readiness, cluster readiness, Owner
  responsiveness, and reconnect/session convergence before taking another
  action.
- Do not repeatedly restart the same Machine. A recurring Fly ingress failure
  calls for replacement on a fresh host; recurring relay pressure calls for an
  application fix. Repeated restarts only create repeated user-visible blips.
- During unattended monitoring, do not deploy, resize, destroy Machines, change
  configuration, or run load tests. Record every intervention, its evidence,
  and the post-action verification.

## Metrics

Fly scrapes `/metrics` every 15 seconds when the deployment adapter's metrics
configuration is enabled. Custom series are local to a Machine and receive Fly
labels such as app, region, host, and instance. Do not add `serverId` or
`connectionId` as labels; their cardinality is unbounded.

Start with dashboards and alerts for:

- `paseo_relay_ready == 0` or `paseo_relay_draining == 1`;
- active WebSockets approaching the deployment's configured soft limit;
- allocated file descriptors above 70% of the Machine limit;
- sustained high memory, CPU, scheduler pressure, or network throughput;
- Machine exits, OOM kills, and unhealthy checks;
- unexpected spikes in reroutes or WebSocket reconnects;
- any increase in `paseo_relay_connection_rejections_total`.

Fly's managed Grafana provides dashboards, but alert delivery needs a separate
Grafana/Alertmanager setup. The next metrics needed for incident diagnosis are
low-cardinality counters for rejected upgrades, close reasons, ownership
takeovers, and cluster peer count.
