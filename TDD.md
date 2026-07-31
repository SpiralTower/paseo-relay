# TDD evidence

## Native Cowboy delivery and admission reshape

- Red: the standard WebSock boundary could not suspend TCP reads while keeping
  the same WebSocket process available for outbound frames. The first
  bidirectional pressure test deadlocked both directions, and the initial fix
  required local patches in two server dependencies.
- Green: the relay now implements Cowboy's native `:cowboy_websocket` behavior.
  `{active, false}` suspends source reads while `websocket_info/2` continues
  outbound delivery; `{active, true}` rearms reads only after the destination's
  synchronous HTTP/1 write barrier. The local dependency patches and their
  verification machinery are deleted.
- Full-duplex evidence: a real daemon-data WebSocket is held under sustained
  fanout pressure by an unread client. While Cowboy keeps that daemon's source
  reads passive, a healthy client sends reverse traffic through the daemon's
  Writer and the same raw daemon socket receives it before pressure is released.
- Buffered input evidence: the production handler charges every completed
  message callback against one global weighted byte budget before delivery. A
  real TCP test sends three frames in one write, observes that Cowboy's
  one-message activation reserves exactly one delivery at a time, attaches the
  destination, and receives all three in order as reads are rearmed. A separate
  five-source test fills the weighted budget with four complete maximum
  messages and proves the fifth closes with `1013 Relay ingress capacity`
  without crossing the cap.
- Protocol evidence: the first raw-TCP red declared a masked payload one byte
  above the legal `32 MiB - 14 bytes` maximum. Cowboy accepted it because the
  initial reshape confused Cowboy's payload limit with the existing 32 MiB wire
  limit. The corrected limit delivers the exact maximum as both unfragmented
  and fragmented binary messages, permits interleaved ping/pong, and closes one
  byte more with `1009`; the relay ingress reservation remains zero.
- Honest boundary: Cowboy's public handler sees only complete messages. There is
  no pre-payload application admission callback or fixed fragmented-assembly
  deadline in this implementation. Incomplete assembly is bounded by Cowboy's
  message limit, the per-WebSocket heap fuse, and the node memory
  watermark. A raw peer sends a non-final fragment, waits, and still receives an
  interleaved pong while the completed-message reservation remains exactly zero;
  this is the direct evidence that a fixed stalled-fragment deadline cannot be
  claimed at the public Cowboy handler boundary. Tests and operations
  documentation no longer claim otherwise.
- Red: with the watermark forced below current BEAM memory, an admitted raw
  socket retained a 1 MiB non-final fragment outside the completed-message
  budget and remained open because the pressure authority tracked only blocked
  deliveries. Green: pressure now monitors every admitted socket, prioritizes
  blocked delivery sources, and otherwise sheds the oldest active socket. The
  same incomplete-fragment peer receives `1013 Relay memory pressure`, active
  WebSockets return to baseline, and ingress reservations remain unchanged.
- Destination evidence: an unread raw TCP destination holds one Writer delivery,
  source reads stay suspended, and the Writer records its deadline before the
  longer transport deadline closes that destination. Healthy fanout remains
  ordered. A completely unread TCP path cannot be guaranteed to carry a queued
  WebSocket close frame; the operations contract now states that physical
  boundary instead of promising `1013` after the transport is already blocked.
  Writer death closes its real Cowboy WebSocket, and unread control traffic is
  bounded by the same Writer rather than a second unbounded mailbox path.
- Outbound-boundary audit: a red metric assertion showed that the compatible
  JSON pong was the last application frame returned directly from Cowboy: the
  peer received it, but `frames_forwarded` did not advance. Control input now
  asks the session Owner for its attached Writer, and that Writer performs the
  bounded send and barrier; the focused test observes one forwarded frame and
  its exact encoded byte count.
- Configuration evidence: listener, delivery, transport, control queue, attach,
  budget, heap, and watermark settings are validated once into a single config
  struct. Validation requires `delivery_timeout_ms < transport_send_timeout_ms`
  so application shedding is recorded before the transport fallback. The final
  standards audit removed the Listener's fallback read of application globals
  and the application's duplicated listener option projection; startup now
  passes the one validated config struct as the sole production shape.
- Admission restart evidence: killing the ingress-budget process initially left
  an existing real Cowboy socket open with reservations erased. Sockets now
  monitor that process and close with `1013 Relay ingress capacity unavailable`;
  the supervised replacement admits a new real WebSocket at the zero baseline.
- Heap-fuse lifecycle evidence: a real Cowboy socket was killed while unmasking
  a payload under a deliberately low heap limit. Before the fix, the externally
  monitored capacity slot returned to zero while `active_websockets` remained
  stuck at one because `Socket.terminate/3` never ran. The gauge now reads the
  connection-budget monitor's attached-socket state, so both capacity and the
  public metric reconcile after forced termination and after budget restart.
- Cleanup evidence: full-suite seed `441893` exposed that killing the WebSockex
  test client could leave Cowboy passive until the kernel close became visible,
  cascading a non-zero active-socket gauge into later tests. Test teardown now
  uses the public WebSocket close behavior and waits for the close handshake.
  A later seed-1 red found the same kill-based cleanup in the protocol fixture:
  one active socket survived its five-second assertion window and caused 27
  subsequent zero-baseline failures. That fixture now uses the same synchronous
  public close path. Seed `441893` then exposed a separate raw-TCP integration
  fixture that closed its transport without a WebSocket close frame and did not
  wait for Cowboy termination. It now sends a masked close frame and observes
  the active-socket gauge return to its exact baseline. A subsequent seed-1 run
  found one behavior test still killing a client process to trigger detach; it
  now triggers the same detach through the peer's public close handshake. The
  protocol fixture also uses a unique session ID per test rather than assuming
  an ephemeral listener port cannot be reused during the Owner idle grace.
- Late-upgrade cleanup evidence: full-suite seed 1 found that the load client's
  deliberately delayed sibling could return from its real Cowboy handler after
  the test's instantaneous zero check. The fixture now monitors that actual
  connection process through termination before checking the public active
  metric, preventing a late upgrade from escaping into the next test module.
- Attach-before-detach evidence: WebSockex reports its open callback after HTTP
  101, which can precede Cowboy's `websocket_init`. A last-client test could
  therefore race data-socket reservation attachment and correctly receive the
  initialization fail-close instead of the intended detach close. The test now
  relays a frame from data to client first, proving public attachment before it
  triggers and asserts last-client cleanup.
- Destination-death fixture evidence: repeated seed `441893` reproduced a peer
  transport closing before its explicit `1012`/`1013` frame. The test closed the
  destination as soon as one of five multi-megabyte sources reached the handler, leaving
  other client payloads unread at Cowboy's transport boundary. The fixture now
  first observes all five complete-message callbacks and real downstream
  backpressure, then kills the destination and requires an explicit close from
  every source.
- Multi-node evidence: a real server-data WebSocket owns a session on one peer;
  a client upgrade first lands on a disjoint peer, receives the Syn owner's
  opaque `409` reroute target, then replays to the owner and exchanges ordered
  text and binary frames in both directions. Relay payloads remain local to the
  owner node.
- Native Cowboy black-box stress evidence, all with zero loss, reordering,
  send, connection, and cleanup failures:
  - High-rate: 200 pairs sent and received 574,800 ordered 1 KiB frames in 30
    seconds at 19,159 steady-state frames/s; p99 latency was 9 ms and relay peak
    RSS was 209,076,224 bytes.
  - Reconnect: 500 pairs completed ten reconnect cycles, 11,001 cumulative
    successful WebSockets, and 20,000 ordered frames; relay peak RSS was
    326,402,048 bytes.
  - Capacity: 15,000 distinct real WebSockets connected successfully in 2.868
    seconds; relay peak RSS was 1,300,512,768 bytes and every connection cleaned
    up. This replaces the pre-Cowboy 642 MB capacity measurement.
  - Large frame: two bidirectional bursts transferred four near-maximum legal
    frames, 134,217,292 payload bytes total, without loss or reordering.
  - Sustained plateau: 100 pairs sent and received 1,178,000 ordered 4 KiB
    frames over five minutes, 4,855,494,600 bytes in each direction. Relay peak
    RSS was 196,116,480 bytes, final loaded RSS was 123,551,744 bytes, and RSS
    after socket cleanup was 114,491,392 bytes. The final BEAM total and binary
    memory readings after cleanup were 82,791,065 and 10,932,528 bytes;
    `active_websockets`, ingress reservations, in-flight delivery, and
    backpressured-source gauges all returned to zero.
- Final native-Cowboy full-suite matrix, all 66 tests: seed 1 passed in 119.4
  seconds, seed 441893 passed in 115.2 seconds, and seed 8191 passed in 114.4
  seconds. Result: 198/198 across three complete seeded runs. The
  destination-death case also passed 20 consecutive repetitions and the
  attach-before-detach case passed 30 consecutive repetitions.
- Final native-Cowboy gates: `mix format --check-formatted`, forced
  `mix compile --warnings-as-errors`, `mix deps.unlock --check-unused`, and
  `git diff --check` all exited zero. `MIX_ENV=prod mix release --overwrite`
  assembled `paseo_relay-0.1.0`; the release was not started or deployed.
- Completion gate after the all-socket pressure and externally-owned gauge
  fixes: the complete suite passed 68/68 in 99.4 seconds. Warnings-as-errors
  test and production compilation, formatting, lock validation, release build,
  and a live release `/health` + `/ready` smoke all passed. A production-release
  load smoke completed 1,950/1,950 sustained frames, 151 reconnect-wave socket
  opens, and 200/200 ownership sockets with zero connection, send, ordering, or
  cleanup failures.
- Hosted-CI red: the clean Ubuntu runner had no EPMD daemon, so the distributed
  test bootstrap failed at `Node.start/2` before ExUnit ran. The test boundary
  now starts the OTP-provided EPMD daemon explicitly before starting its named
  node, matching the fresh-runner environment instead of depending on a daemon
  left running by local development.
- The same clean dependency fetch reported fixed memory-exhaustion advisories in
  Cowboy 2.17 and Cowlib 2.18. The lock now resolves Cowboy 2.18, Cowlib 2.19,
  and Ranch 2.2.1; `mix hex.audit` exits zero and is part of the mandatory CI
  path. The two remaining ignored Cowlib advisories concern unused client-side
  cookie encoding and invalid response-header construction rejected by Cowboy,
  as documented beside the existing allowlist in `mix.exs`.
- Second review-cycle red: a real v2 control socket accepted and decoded a
  64 KiB+1 text message instead of rejecting it before JSON allocation. A real
  Cowboy socket killed by its configured max-heap fuse during an active delivery
  returned the active-WebSocket and ingress gauges to zero but permanently left
  one backpressured source and seven in-flight bytes. Killing the old ingress
  singleton also allowed its empty replacement to exist while old connections
  were still closing.
- Green: `PaseoRelay.Capacity` is now the single monitored ledger for connection
  reservations, attached sockets, explicit per-message tokens, retained bytes,
  delivery transitions, pressure order, and all four transient gauges. The
  heap-fuse boundary returns every gauge to baseline without running
  `Socket.terminate/3`. A 64 KiB Cowboy control ceiling returns `1009` before
  parsing, while supported control messages enter the same ledger as data.
  Capacity and the production Ranch listener run under `:rest_for_one`; a real
  retained-payload restart test observes the old listener terminate before
  replacement admission succeeds.
- Hosted-CI red: the global-count reconnect assertion included unrelated Owners
  that expired during its 1,000-server wave, and later peer tests could inherit a
  changed node cookie. The test now preserves the cookie, checks every named
  surge Owner through Syn on all three real nodes, and waits for the public
  reroute result before attempting a disjoint-node WebSocket upgrade.
- Final review red: a queued reservation-expiry message could remove a socket
  after its reservation had already become active. A socket chosen for pressure
  shedding could also admit or start pipelined messages before its asynchronous
  close ran. A separate real-socket red proved one pressure check shed only one
  of two sockets despite more than one maximum message of memory overshoot.
  Expiry now applies only to reservation state, and shedding is a terminal
  ledger state: both new admission and reserved-to-delivering transitions fail
  closed with token cleanup and an orderly `1013`. Pressure checks shed a batch
  proportional to current overshoot (capped at 64) and recheck after 100 ms while
  pressure and eligible sockets remain.
- The public heap-fuse regression establishes a real 8 MiB delivery to an unread
  TCP destination, then sends a maximum legal frame in the reverse direction to
  kill the actual Cowboy process. It passed four consecutive focused runs and
  reconciled active sockets, retained bytes, in-flight bytes, and blocked sources
  each time. The final local gate passed formatting, warnings-as-errors test
  compilation, unused-lock validation, and all 73 tests in 101.2 seconds.
- Clean Linux CI red completed 70/73: the large-frame full-duplex and heap tests
  queued reverse sends behind an 8 MiB WebSockex receive, while the maximum-frame
  digest callback missed its 15-second runner deadline. The runtime boundary was
  healthy; the secondary client scheduling made the evidence platform-dependent.
  The tests now wait for the healthy WebSockex peer to finish the forward frame
  before sending reverse traffic, receive the maximum frame through the existing
  raw WebSocket boundary, and use a bounded deadline while awaiting the real
  max-heap transport close. All three focused tests passed four consecutive local
  runs after the correction; the post-correction complete suite passed 73/73 in
  97.5 seconds with formatting, warnings-as-errors compilation, and lock
  validation green.
- The next clean Linux run closed the unread-control test with `1012 Session
  expired` before its target scenario: constructing 1,000 existing session
  members first exhausted the five-second upgrade reservation under runner
  scheduling. The test now establishes the unread control socket first and then
  fills its real transport with the same notification wave, isolating bounded
  Writer shedding from session-upgrade latency. The focused regression passed
  four consecutive local runs, followed by a 73/73 complete-suite pass in 98.7
  seconds.
- A subsequent Linux red armed the one-byte watermark before its blocked source
  existed; the periodic checker correctly shed the new active socket first, so
  the test never observed one blocked source. Both blocked-delivery and retained-
  fragment tests now prove their public precondition before enabling pressure and
  invoking the explicit check. The focused pair passed four consecutive runs
  without a timing sleep, followed by a 73/73 complete-suite pass in 98.6 seconds.
- The following hosted run passed all 73 tests, both production relay image
  builds, the load-client image build, generic-container health/readiness, and
  the sustained, reconnect, and ownership smokes, then failed only when the Fly
  adapter container did not become healthy. CI supplied the app and Machine
  identifiers but omitted Fly's private IPv6 address, leaving the adapter's
  IPv6 distribution mode without the provider node identity it translates in
  production. The artifact probe now supplies loopback IPv6 as `FLY_PRIVATE_IP`
  and prints container logs if readiness fails, exercising the real adapter
  contract while keeping failures diagnosable.
- That diagnostic boundary exposed the next clean-runner red before the release
  started: GitHub's default Docker hard `nofile` limit was below the Fly
  adapter's documented 100,000 descriptor requirement, so its mandatory
  `ulimit` exited with `Operation not permitted`. The Fly artifact probe now
  starts its container with the same 100,000 soft and hard descriptor limit as
  the deployment contract rather than testing an incompatible Docker default.

### Final standards architectural audit

- Dependency and transport boundary: the vendored Bandit and Thousand Island
  trees, patch verifier, Plug router, and custom WebSock returns are gone. The
  only server path is public Cowboy/Ranch HTTP/1 with native active-mode flow
  control.
- Protocol and admission boundary: the 32 MiB masked data ceiling and 64 KiB
  inbound-control ceiling are derived in one protocol module. One capacity
  ledger owns connection and message admission, delivery state, pressure order,
  and computed gauges; its socket monitors reconcile forced exits. Ledger loss
  stops every production listener connection before a fresh epoch admits work.
- Session and distribution boundary: Owner remains the only session topology
  authority, Syn stores only `serverId` ownership metadata, and the disjoint-node
  test proves reroute before payload exchange. No payload path uses OTP
  distribution or a node-wide Registry.
- Delivery boundary: Writer is the sole application-frame sender, including
  JSON pong and Owner notifications. Its payload waiters contain metadata only;
  completed payload references remain in their tokenized source/task owners, and
  its control payload queue has an explicit byte cap. Writer, Owner, connection
  Owner and capacity-ledger death all fail sockets closed.
- Configuration and provider boundary: runtime parsing validates one config
  struct, startup passes that struct without duplicated listener defaults, and
  the Writer deadline must precede the transport send timeout. Core code has no
  provider reference; Fly behavior and replay tooling remain under
  `deployment/fly`.
- Test and operations boundary: behavior uses real TCP, Cowboy, WebSockets, and
  peer BEAM nodes. Capacity documentation now records the native-Cowboy 15,000
  socket peak instead of extrapolating the earlier server implementation.
- Unavailable public boundary: Cowboy does not expose pre-payload admission or
  a fixed first-fragment-to-completion deadline. The implementation and tests do
  not claim either guarantee; incomplete assembly remains under Cowboy's size
  limit, the heap fuse, and optional memory watermark.

## v1 pairing

- Red: `mix test test/relay_protocol_test.exs` failed with `WebSockex.RequestError{code: 404}` because the bootstrap router had no `/ws` upgrade route.
- Green: the same real WebSocket test passed after adding query validation, the upgrade, and the single-node session registry.

## v2 control and buffering

- Red: the first control assertion failed because it compared serialized JSON rather than the control message it represents.
- Green: the test now decodes the real received control frame and verifies the `sync` and `connected` messages, then verifies ordered buffered text and binary delivery after the daemon data socket connects.

## duplicate daemon data

- Red: `mix test test/relay_protocol_test.exs:102` timed out waiting for the replacement daemon data socket after the displaced socket's termination deleted the new route.
- Green: the registry now deletes a v2 data route only when its current owner disconnects; the focused real-WebSocket test passes.

## distributed ownership and reroute

- Red: the real peer-node test exposed that tying ownership to the first request could move an otherwise active session when that request process exited.
- Green: a per-`serverId` owner now reserves upgrades, monitors every attached WebSocket, expires abandoned reservations, and remains authoritative until the whole session is idle. Real `:peer` tests cover concurrent claims, remote lookup, owner loss, and takeover.
- Red: the pre-upgrade router test reached WebSocket negotiation on a non-owner node.
- Green: the non-owner now returns the configured opaque reroute response before upgrade; the owner still completes a real WebSocket handshake.

## live partition healing

- Red: the first real two-node partition fixture used OTP's default fully
  connected topology. `:global` correctly prevented the overlapping partition
  by disconnecting the remaining links, so the test lost its control path
  before it could exercise Syn's conflict resolution.
- Green: the peers now use the same explicit-connect topology as Syn's own
  network-partition suite. Two real Cowboy listeners accept the same
  `serverId` while disconnected; after reconnect, all observers converge on
  one owner, the losing WebSocket receives `1012 Session owner moved`, and a
  new WebSocket upgrade on the losing listener receives a `409` reroute to the
  winner. The focused test passed three consecutive seeded runs.

## owner call pressure tolerance

- Red: a real owner process paused for 1.1 seconds caused `Owner.reserve/1` to
  return `:closed` even though the process was still healthy, because its local
  `GenServer.call` used a one-second timeout.
- Green: owner coordination now uses the standard five-second local call
  bound. The same paused owner resumes and returns a valid reservation. Registry
  attachment remains at five seconds; the 15,000-WebSocket run already exercises
  that shared mailbox, so its timeout was not widened to mask overload.

## public identifier bounds

- Red: a 257-byte `serverId` completed a `101 Switching Protocols` response and claimed distributed ownership.
- Green: identifiers longer than 256 bytes now receive `400` before ownership, while empty client connection IDs retain the compatible generated-ID behavior.

## sharded load generation

- Red: a real black-box run requesting `--no-control` still opened five WebSockets for two pairs because every load process unconditionally opened its own daemon control socket.
- Green: sharded runs can omit that single shared socket and use an explicit connection-ID prefix. A real WebSocket test verifies four data sockets, bidirectional frames, and clean shutdown without importing relay internals.
- Red: the same real-server test had no keepalive accounting when a keepalive interval was requested.
- Green: every open test socket can now send a small, separately-counted keepalive frame during long ramps; timers are cleared on close and finalization.

## Relay parity hardening (`6dbe13c`)

### Reject invalid upgrades before ownership

- Red: `mix test test/paseo_relay/router_integration_test.exs:18` sent a plain
  `GET /ws` request and reached ownership before WebSocket upgrade validation.
- Green: the same real TCP request receives `426 Expected WebSocket upgrade`
  and `Ownership.owner_pid/1` returns `:undefined`.

### Legacy JSON control keepalive

- Red: `mix test test/relay_protocol_test.exs:77` sent `{"type":"ping"}` on a
  real v2 control WebSocket and timed out waiting for a response.
- Green: the same socket receives a JSON object with `type: "pong"` and an
  integer timestamp.

### Stuck control recovery

- Red: `mix test test/relay_protocol_test.exs:94` connected a client without a
  matching server-data socket; after 11 seconds, control had received no sync
  nudge.
- Green: control receives the current `sync` list at 10 seconds and, when data
  is still absent at 15 seconds, closes with `1011 Control unresponsive`.

### Registry crash fail-closed behavior

- Red: after a verified client-to-data frame, killing the registered Registry
  left the real client WebSocket open past one second.
- Green: each socket monitors the Registry process that attached it; the same
  process-level crash closes client and data WebSockets with
  `1012 Registry unavailable`.

### No relay idle disconnect

- Red: the real idle-WebSocket regression received a remote close after 60
  seconds because the listener retained its default idle timer.
- Green: the Cowboy WebSocket and protocol listener use `idle_timeout: :infinity`.
  The same real socket
  remains open past 61 seconds.
- Verification correction: ExUnit's default per-test timeout is also 60
  seconds, so the regression test is explicitly tagged `timeout: 75_000`.
  The assertion remains a real idle socket held open for 61 seconds.

### Honest load-test cleanup accounting

- Red: a live load run received clean `1000` closes after roughly 5.3 seconds,
  beyond the harness's fixed 5-second cleanup window, and reported them as
  connection failures. The public-contract tests also failed while
  `--cleanup-grace` and `cleanup_timeouts` were absent.
- Green: teardown now has a configurable 15-second default grace and reports
  sockets that outlive it separately as `cleanup_timeouts`. A cleanup timeout
  still fails the run, while the existing close listener continues to count
  abnormal closes such as `1006` as connection failures. The real-server load
  test verifies successful teardown through the public JSON result. A live
  201-WebSocket run then completed with zero failures or cleanup timeouts even
  though teardown took about 10.3 seconds, beyond the previous fixed window.

### Failed setup owns pending sockets through teardown

- Red: a real relay endpoint delayed the server-data upgrade while a second
  real HTTP endpoint rejected its matching client upgrade. The CLI printed the
  setup failure but did not exit within the test's three-second bound because
  the sibling opened after cleanup had snapshotted only already-open sockets.
- Green: every created socket now has a completion lifecycle before its upgrade
  settles. Finalization marks pending siblings, closes them if they subsequently
  open, and waits for their completion. The same real-network test exits in
  about 600 milliseconds with a failed status and non-`101` error, no cleanup
  timeout, and zero active relay WebSockets. After bounded cleanup, the CLI
  explicitly exits so a transport stuck below the WebSocket API cannot retain
  the load process indefinitely.

## Complete malformed-handshake validation

- Red: `mix test test/paseo_relay/router_integration_test.exs:28` sent
  `Upgrade: websocket` without `Connection: Upgrade`, reached routing, and
  created an owner before the handshake failed.
- Green: the Cowboy handler checks `is_upgrade_request/1` before
  `Ownership.route/2`; the same real TCP request returns `426` and ownership
  remains `:undefined`.

## Ownership surge and bounded admission

### Replace synchronous global ownership

- Red: the production-shaped three-node `:global` path returned `503 owner` for
  a 500-session reconnect surge. Locally, 10,000 distinct ownership claims took
  roughly 14.6 seconds. Removing the redundant transaction and `global.sync/0`
  still took roughly 13 seconds because `global.register_name/2` is itself a
  synchronous cluster-wide registration.
- Green: ownership now uses Syn's strict distributed registry and advertises the
  opaque reroute target as registration metadata. A real three-node BEAM test
  covers concurrent conflicts, convergence, owner loss, remote takeover, and
  distinct-server surges. The same local machine completed 10,000 claims in
  roughly 200 milliseconds and 50,000 in roughly 1.17 seconds.
- Safety: Syn may briefly admit competing owners during a race or partition.
  Every real relay socket monitors its owner and closes with `1012 Session owner
  moved` if Syn discards that owner. The real WebSocket regression was red
  before owner monitoring and green afterward.

### Exercise ownership through real WebSockets

- Red: the black-box load client rejected `--scenario ownership`; it could only
  create many connections under one shared `serverId`, so it did not exercise
  the distributed ownership bottleneck.
- Green: the scenario opens one real v2 daemon-control WebSocket for every
  distinct `serverId`, in bounded batches, through the public `/ws` contract. A
  committed real-server test opens 1,000 distinct sessions. A manual local run
  opened 15,000 real WebSockets in 1.50 seconds with zero connection, send, or
  cleanup failures. This was the pre-Cowboy implementation; relay RSS was about
  602–642 MB across repeated runs.
- Load-generator boundary: a single macOS source/destination tuple has 16,384
  ephemeral ports (`49152..65535`), so a 50,000-socket attempt failed in the
  client around 16.3k connections. The 50,000-owner three-node BEAM test and the
  15,000 real-socket test measure the two layers without misreporting client port
  exhaustion as relay failure.

### Enforce the active-WebSocket ceiling explicitly

- Red: Ranch removes upgraded connections from its connection accounting when
  Cowboy takes over the WebSocket. A Ranch `max_connections` setting therefore
  limits concurrent HTTP handling, not the relay's long-lived WebSockets.
- Green: a node-local active-WebSocket budget admits exactly the configured
  number of real upgrades. The next valid local upgrade receives `503 Relay
  connection capacity` and increments the rejection counter. Closing an
  admitted WebSocket releases its monitored slot, after which the rejected
  session completes a real `101` upgrade. The test finishes at the exact zero
  baseline, covering normal and monitored cleanup rather than process internals.
- Fail-closed evidence: every admitted WebSocket monitors the budget process.
  A real socket receives `1013 Relay capacity unavailable` when that process is
  killed; after supervision replaces the budget, a new real upgrade succeeds
  and its slot returns to zero on close. A budget restart therefore cannot erase
  admission counts while old sockets remain unaccounted.

### Fail closed across owner and metrics races

- Red: after obtaining a real owner reservation, killing that owner before
  WebSocket initialization made `Owner.attach/3` exit with `:noproc`, crashing the socket
  process before it could install its owner monitor.
- Green: owner calls now translate owner death and call timeout into `:closed`.
  The same reserved-owner regression returns the existing
  `1012 Session expired` close path. Lookup-to-reserve and
  reservation-to-attach use the same bounded call boundary.
- Red: killing `PaseoRelay.Metrics` with `:kill` left the old listener-specific
  telemetry handler registered. The replacement failed with `already_exists`,
  exhausted the application supervisor, and left the relay stopped.
- Green: the native listener has an explicit active-WebSocket admission boundary
  and increments its rejection counter directly, so Metrics owns no external
  telemetry handler. Fault injection now produces a new Metrics PID while the
  original relay supervisor stays alive, `/metrics` returns 200, and counter
  values survive the restart.

### Prove distributed convergence after the ownership surge

- The original surge test asserted only that each landing node returned a local
  owner. Because Syn replication is asynchronous, that measured local owner
  creation plus RPC throughput but not a usable converged registry.
- The strengthened test snapshots per-origin registry counts on every node,
  creates distinct owners round-robin across three real BEAM nodes, and waits
  until every observer sees the exact expected count from every origin. It then
  resolves sampled IDs from a non-owner node and verifies their opaque reroute
  targets.
- With `PASEO_OWNERSHIP_SURGE_COUNT=50000`, registration, full three-node count
  convergence, and cross-node route sampling complete in roughly 1.5 seconds on
  the local test cluster.

### Keep production capacity inside the measured memory envelope

- A real native-Cowboy worst-case run held 15,000 distinct owner WebSockets with
  zero failures at 1,300,512,768 bytes relay peak RSS. The earlier 50,000-owner
  result exercised BEAM
  ownership without network sockets and therefore did not validate the 2 GB Fly
  Machine memory boundary.
- The Fly template treats 10,000 as a placement signal and refuses new proxy
  connections at 15,000; autostart remains disabled. The generic relay rejects
  active-WebSocket admission at 20,000 as a final provider-independent safety
  net. No production deployment threshold exceeds the 15,000 real-socket
  capacity run.

## Deterministic full-suite ownership teardown

- Red: an independent `asdf exec mix test --seed 268085` completed 57/58. In
  `a remote node can claim after the current owner dies`, the monitor delivered
  `{:DOWN, ref, :process, owner, :noproc}` for the expected ref and owner PID,
  while the test required the incidental reason `:normal`. A local pre-change
  rerun of the exact command passed 58/58 in 99.1 seconds, confirming that the
  reason race was intermittent rather than a deterministic behavior failure.
- Green: both owner-death tests accept any reason only for the pinned monitor
  ref and owner PID, then wait through the public ownership API until the
  server resolves `:unowned` before reclaiming it. Production owner behavior is
  unchanged.
- A subsequent full-suite seed-1 red returned the documented retryable
  `{:unavailable, :owner}` during two simultaneous cross-node claims. The test
  helper crashed while decoding that result. It now retries only this explicit
  retryable response through `Ownership.route/2`; all winner PID, liveness,
  convergence, and opaque-target assertions remain intact.
- The same seed-1 run also supplied a real maximum-frame red: the socket
  process reached the configured `16,777,216`-word shared-binary heap fuse while
  unmasking the maximum legal `32 MiB - 14 bytes` client payload. The destination
  received nothing, proving the default fuse could kill protocol-valid traffic.
  The configurable default and minimum are now `33,554,432` words; values below
  that proven-safe floor are rejected. The 32 MiB wire-frame ceiling itself is
  unchanged. The focused real-network/config gate passed 9 tests with 14
  excluded in 0.7 seconds.
- Final full-suite matrix (`asdf exec mix test --seed SEED`), all 59 tests each:
  seed 1, 95.2s; 17, 96.3s; 101, 95.6s; 313, 93.2s; 997, 96.5s; 2027,
  93.8s; 4099, 95.4s; 8191, 95.5s; 16381, 93.5s; and audited seed 268085,
  101.1s. Result: 590/590 passed across 10 full-suite seeds.
- Final gates: `asdf exec mix format --check-formatted` exited 0;
  `asdf exec mix compile --warnings-as-errors` compiled two changed files and
  exited 0; `git diff --check` exited 0; and
  `MIX_ENV=prod asdf exec mix release --overwrite` assembled
  `paseo_relay-0.1.0` successfully. The release was not started or deployed.

## Final admission, deadline, and pressure boundaries

- HTTP lease red/green: with one Ranch connection slot, a `POST /health` that
  declared but withheld its body kept the next health request blocked when the
  protocol idle timeout was infinite. A finite pre-upgrade HTTP idle timeout now
  releases that slot, while a real upgraded WebSocket remains alive beyond the
  same interval and answers ping with pong.
- Ownership admission red/green: a valid upgrade rejected at the WebSocket
  ceiling previously created an Owner before admission and left its `serverId`
  registered during the grace period. Local capacity is now leased before
  reserving or creating an Owner, and a real rejected upgrade leaves its unique
  `serverId` unowned. A second red showed that admitting before even looking up
  ownership made a full landing node return `503` for a healthy remote session;
  known remote owners now retain their `409` reroute path without consuming a
  local slot.
- Absolute deadline red/green: suspending an alive, fully attached Owner made a
  real source delivery wait forever before the Writer deadline began. The source
  now carries one deadline through Owner lookup, data attachment, Writer
  reservation, and the transport write barrier; the real source closes with
  `1013 Delivery unavailable` and every capacity gauge returns to zero.
- Accepted-control red/green: an accepted control notification queued behind a
  blocked Writer could expire and disappear while its socket stayed healthy.
  Controlled Writer and Cowboy scheduling over a real control WebSocket now
  proves the accepted notification is either written before its deadline or
  closes the destination with `1013 Slow consumer`.
- Measured-pressure red/green: after older idle sockets and newer real sockets
  retained incomplete 8 MiB fragments, the previous one-shot estimate admitted
  replacement work before demonstrating relief. A pressure episode now rejects
  new upgrades, prioritizes known blocked deliveries, conservatively chooses
  newest unclassified sockets, and repeats bounded batches based on measured
  BEAM memory until hysteresis recovery. The fragment source closes with `1013`,
  an older idle socket remains responsive, and admission reopens only after
  measured relief.
- Artifact-restart gate: the mandatory CI path now restarts the built generic
  production container after sustained traffic, re-probes its exact health,
  readiness, and metrics contracts, and then opens the bounded 151-socket
  reconnect wave. This covers boot-after-restart plus concurrent real-WebSocket
  behavior without moving the documented 23,000-socket staging gate into every
  pull request.
