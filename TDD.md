# TDD evidence

## Bounded relay delivery and ingress admission

- Red: real raw WebSocket/TCP tests had no backpressure metrics because source
  callbacks returned after an asynchronous Registry cast; the singleton path
  also retained the existing 200-frame pre-data queue.
- Green: payloads bypass the deleted Registry. Real Bandit tests verify a
  passive destination holds one in-flight payload and stalls its source TCP
  sender, pre-data delivery blocks without a payload queue and resumes in exact
  order, five concurrent maximum-frame headers never exceed the 512 MiB
  weighted ingress budget, and reservations return to zero after disconnect.
- Contention: five real concurrent producers retain per-source FIFO order
  through one Writer. A deliberately unread fanout client is closed with
  retryable `1013`, while the healthy client receives the large frame and the
  next ordered frame. Destination death releases every blocked producer,
  Writer, and ingress reservation.
- Red: the first full-duplex black-box burst sent both directions at once and
  timed out all four deliveries: blocking both source callbacks prevented each
  socket from processing the other's queued frame/barrier.
- Green: Bandit/Thousand Island now support a generic suspended handler result.
  Source TCP stays passive while the WebSocket process services outbound
  frames, and resumes only after delivery completion. The same bidirectional
  burst/keepalive test relays frames and tears down cleanly in under one second.
- Safety fuses: real WebSocket tests verify missing daemon data expires with
  `1013` and the BEAM watermark cancels the oldest blocked delivery, releases
  admission, and sends explicit `1013 Relay memory pressure`.
- Dependency: Bandit was updated from vulnerable 1.12.0 to 1.12.3. Bandit and
  Thousand Island are pinned as reviewed local source dependencies; their small
  generic admission/suspension deltas are documented in each
  `PASEO_PATCH.md`.
- Stress: a production release sustained 10 real v2 pairs at 1 MiB per frame in
  both directions for 300 seconds: 5,980/5,980 frames and 6.27 GB/6.27 GB
  sent/received, 19.93 frames/s, zero connection/send/cleanup failures, p99
  latency 71 ms, and final relay RSS 183.5 MB. BEAM total/binary memory sampled
  near 140/46 MB after the initial allocator peak, with zero delivery timeouts
  and no growth ramp.

### Independent backpressure audit remediation

- Red lifecycle evidence: an independent full `mix test` failed
  `the node watermark explicitly closes the oldest blocked source` with
  `active_websockets remained 0` at the baseline assertion. Reproduction with
  `mix test test/relay_backpressure_test.exs --seed 1 --max-failures 1` also
  timed out in the old reduce/sleep slow-fanout loop. The listener had been
  killed asynchronously, so socket metric decrements crossed test boundaries.
  The real-socket harness now synchronously closes every tracked WebSockex/raw
  TCP client, starts each listener under the ExUnit supervisor, monitors process
  termination, and does not return from teardown until active WebSockets, backpressured sources,
  in-flight bytes, ingress reservations, and the Budget total exactly match
  the test's starting values.
- Red seed evidence: repeated execution later found seed `997` leaving
  `active_websockets` at 5 when a test assumed closing a deliberately passive
  TCP peer would be immediately observable. The test now synchronizes on the
  explicit payload-completion deadline/close event; it does not add a sleep or
  extend an assertion timeout.
- Red admission evidence: the old 32 MiB Bandit `max_frame_size` is a wire-byte
  limit, so a 32 MiB unfragmented client payload correctly produces
  `{:error, :max_frame_size_exceeded}` because its 14-byte header exceeds the
  existing contract. `PaseoRelay.Protocol` now centralizes and distinguishes
  the unchanged 32 MiB wire-frame ceiling, its 32 MiB-minus-14 maximum client
  payload, and the separate 32 MiB assembled fragmented-message payload limit.
  Real TCP tests deliver the maximum legal unfragmented payload through the
  heap fuse, deliver an exactly 32 MiB fragmented message with an interleaved
  ping/pong, reject the next oversized unfragmented payload with `1009`, and
  return reservations to zero.
- Red starvation evidence: before the tracked admission lifecycle, four peers
  could advertise maximum frames, send no payload, and retain essentially the
  entire weighted 512 MiB budget indefinitely. A configurable
  `PASEO_RELAY_PAYLOAD_TIMEOUT_MS` now starts when admission is granted, is
  cancelled by complete message delivery, and closes an incomplete peer with
  `1013 Payload completion timeout`. A deterministic five-source raw-TCP test
  fills admission with four stalled headers and proves their expiry releases
  the queued fifth source, whose complete maximum legal payload is delivered.
- Metric compatibility: Writer increments forwarded frames/bytes for every
  actual destination handoff, so one two-client fanout increments frames by 2
  and bytes by twice the payload. Owner control notifications retain the same
  per-destination contract. Both are covered through real WebSockets.
- Deterministic fanout: an unread raw TCP/WebSocket peer uses bounded receive
  and relay send buffers. A fixed sequence of 8 MiB frames synchronizes each
  source resume on healthy delivery and `backpressured_sources == 0`; the real
  Writer/TCP deadlines shed and clean up the unread peer while the healthy
  client remains ordered. No Owner state inspection, process suspension,
  reduce/retry loop, or sleep remains. Per-destination metric compatibility is
  covered separately by two healthy real WebSockets.
- Red Writer-deadline evidence: the first final seed matrix failed seed `8191`
  with `slow_consumer_disconnects remained 0`. The fanout coordinator killed a
  per-destination task at the same instant as the Writer's reservation timer;
  the Writer could observe caller death first and release the reservation
  without shedding the stalled destination. Writers now exclusively own their
  bounded deadlines, and the coordinator synchronously collects their bounded
  results instead of racing them with a duplicate timer. The formerly failing
  seed and the complete matrix below pass.
- Final repeated command: seeds `1 17 101 313 997 2027 4099 8191 16381 32749`
  each ran `mix test test/relay_backpressure_test.exs --seed SEED`; every run
  reported `Result: 15 passed` (150 executions, zero failures).
- High-message-rate command: `node scripts/relay-load.mjs --endpoints
  ws://127.0.0.1:51005/ws --server-id high-rate-owned-deadline --pairs 50
  --batch-size 25 --scenario sustained --duration 30 --rate 125
  --payload-bytes 128 --drain-timeout 30 --relay-pid 30758` delivered
  345,400/345,400 ordered frames at 11,513.30 steady frames/s with zero loss,
  ordering, send, connection, cleanup, or delivery failures. Relay peak/final
  RSS was 197,476,352/193,773,568 bytes. The preceding 100-rate run reached
  only 9,206.25 frames/s and was not counted as satisfying the 10k target.
- Reconnect command: `node scripts/relay-load.mjs --endpoints
  ws://127.0.0.1:51005/ws --server-id reconnect-owned-deadline --pairs 100
  --batch-size 100 --scenario reconnect --reconnects 20 --burst 100
  --duration 0 --payload-bytes 128 --drain-timeout 30 --relay-pid 30758`
  opened 4,201 real sockets cumulatively and delivered 20,000/20,000 frames
  with zero loss, reordering, connection, send, or cleanup failures; relay
  peak/final RSS was 218,529,792/218,529,792 bytes.
- Capacity command: `node scripts/relay-load.mjs --endpoints
  ws://127.0.0.1:51005/ws --server-id capacity-owned-deadline --scenario
  ownership --servers 15000 --batch-size 500 --duration 30 --relay-pid 30758`
  opened all 15,000 requested real WebSockets in 1.932 seconds, held them for
  30 seconds, and closed with zero connection or cleanup failures. Relay
  peak/final RSS was 694,861,824/694,861,824 bytes; the local ephemeral-port
  boundary was not hit.
- Dependency suites: patched Thousand Island 1.5.0 completed `98 passed` after
  one retry of an upstream 500 ms telemetry assertion. Patched Bandit 1.12.3's
  standard suite completed `715 passed, 1 skipped, 3 excluded`. Running all
  slow external suites produced `715/718 passed, 1 skipped`: Autobahn 6.1.2
  rejects zero-length non-final continuations, Docker h2spec cannot connect to
  the macOS/ARM host listener, and one HTTP/2 WINDOW_UPDATE test receives an
  intervening headers frame. The latter fails identically against the pristine
  1.12.3 tag; h2spec also fails pristine, and the Autobahn report exercises an
  unchanged upstream zero-length-fragment policy. No external fork was made.
- Full-suite red: after the backpressure cases were stable, `mix test` exposed
  an existing Syn observation race in `a remote node can claim after the
  current owner dies`: the local observer returned `:unowned` immediately
  after the remote claim. The assertion now waits on the already-established
  `await_resolve/3` convergence condition. Ten seeded focused peer runs passed.
- Final relay command: `mix test` reported `Result: 58 passed` in 95.2 seconds.

### Final standards audit corrections

- Fragmented-assembly red: after an 8 MiB non-final binary fragment and an
  interleaved ping/pong proved that Bandit had completely parsed the fragment,
  `mix test test/relay_backpressure_test.exs:175 --seed 0` timed out waiting for
  `1013`; the weighted reservation remained held. Bandit's generic extractor
  now reports the admission that completed a frame, and its WebSocket handler
  retains the original timer while `fragment_frame` remains open. The timer is
  cancelled only when the final assembled message reaches WebSock or the
  connection closes. The same real raw-TCP test receives
  `1013 Payload completion timeout`, observes the exact zero reservation
  baseline, and delivers subsequent legitimate traffic.
- Budget-boundary red: `mix test test/paseo_relay/config_test.exs:89 --seed 0`
  accepted `167772159` bytes at weight 5, one byte below the legal 32 MiB
  assembled-message requirement. Validation now uses
  `Protocol.maximum_message_payload_bytes() * weight`; exactly `167772160`
  passes and one byte below fails with the explicit assembled-message error.
- Real-TCP fanout red: replacing `:sys.get_state`/`:sys.suspend` initially
  reproduced `slow_consumer_disconnects remained 0` because macOS loopback
  could accept one 16 or 32 MiB write. Overlapping two maximum messages then
  correctly tripped the heap fuse, proving that was not a valid pressure setup.
  The final fixed-volume raw-TCP producer sends eight sequential 8 MiB frames,
  synchronizing every send on healthy receipt and source resume. It deterministically
  reaches the unread peer's send deadline, observes its exact active-socket
  cleanup, and delivers the post-close frame in order.
- Harness red: a seeded run left one suspended 4 MiB delivery during manual TCP
  teardown, and a full run later captured a transient `1 WebSocket / 8 weighted
  bytes` as a new baseline. Passive pressure now exits through real bounded
  Writer/TCP deadlines. Every listener is explicitly stopped in the tracked
  LIFO resource stack, and setup waits for the known all-zero global transient
  state before and after every test instead of ratcheting a transient snapshot.
- Reproducible dependency command:
  `scripts/verify-patched-dependencies.sh` verifies Bandit 1.12.3 commit
  `6e2ab9cce4869759809da0a2bce6e6e081cf80ae` and Hex SHA-256
  `a253ec03f391755b2126e4181ee2fee05c75b712b407aa399de1831b5088c58e`,
  plus Thousand Island 1.5.0 commit
  `62223053915edcc3beaa6ed8e43e3bfc1217fd7c` and Hex SHA-256
  `708923d40523e43cf99041ab37a0d4b0ec426ac6438fa3716ab23d919eaeb412`.
  It reconstructs and compares both vendored trees, then reports Thousand
  Island `98 passed` and Bandit `715 passed, 1 skipped, 3 excluded`.
  `scripts/verify-patched-dependencies.sh --include-slow` reports the same
  three previously reproduced upstream/environment failures and
  `715/718 passed, 1 skipped`; no external repository or fork is created.
- Final high-rate command: `node scripts/relay-load.mjs --endpoints
  ws://127.0.0.1:51006/ws --server-id high-rate-fragment-deadline --pairs 50
  --batch-size 25 --scenario sustained --duration 30 --rate 125
  --payload-bytes 128 --drain-timeout 30 --relay-pid 98342` delivered
  345,100/345,100 ordered frames at 11,502.91 steady frames/s with zero loss,
  ordering, connection, send, or cleanup failures; relay peak/final RSS was
  193,576,960/191,152,128 bytes.
- Final reconnect command: `node scripts/relay-load.mjs --endpoints
  ws://127.0.0.1:51006/ws --server-id reconnect-fragment-deadline --pairs 100
  --batch-size 100 --scenario reconnect --reconnects 20 --burst 100
  --duration 0 --payload-bytes 128 --drain-timeout 30 --relay-pid 98342`
  opened 4,201 real sockets cumulatively and delivered 20,000/20,000 ordered
  frames with zero loss, connection, send, or cleanup failures; relay peak/final
  RSS was 214,171,648/214,171,648 bytes.
- The 15,000-WebSocket capacity result remains applicable: this audit changed
  admitted message assembly, budget validation, and tests, not connection setup
  or the per-socket Writer process. The already-recorded final-branch run opened
  15,000/15,000 sockets with zero failures at 694,861,824 bytes peak/final RSS.

## v1 pairing

- Red: `mix test test/relay_protocol_test.exs` failed with `WebSockex.RequestError{code: 404}` because the bootstrap router had no `/ws` upgrade route.
- Green: the same real Bandit/WebSocket test passed after adding query validation, the WebSock upgrade, and the single-node session registry.

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
- Green: the non-owner now returns the configured opaque reroute response before upgrade; the owner still completes a real Bandit WebSocket handshake.

## live partition healing

- Red: the first real two-node partition fixture used OTP's default fully
  connected topology. `:global` correctly prevented the overlapping partition
  by disconnecting the remaining links, so the test lost its control path
  before it could exercise Syn's conflict resolution.
- Green: the peers now use the same explicit-connect topology as Syn's own
  network-partition suite. Two real Bandit listeners accept the same
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
- Green: sharded runs can omit that single shared socket and use an explicit connection-ID prefix. A real Bandit/WebSocket test verifies four data sockets, bidirectional frames, and clean shutdown without importing relay internals.
- Red: the same real-server test had no keepalive accounting when a keepalive interval was requested.
- Green: every open test socket can now send a small, separately-counted keepalive frame during long ramps; timers are cleared on close and finalization.

## Relay parity hardening (`6dbe13c`)

### Reject invalid upgrades before ownership

- Red: `mix test test/paseo_relay/router_integration_test.exs:18` sent a plain
  `GET /ws` request and received `500 Internal Server Error` from
  `WebSockAdapter.UpgradeError` after `Ownership.route/2` had run.
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

- Red: the real idle-WebSocket regression ran with `timeout: nil` and received
  a remote `1002` close after 60 seconds. Bandit treats `nil` as no timeout
  override, so ThousandIsland retained its server-level 60-second read timer.
- Green: the route passes `timeout: :infinity`, which ThousandIsland handles as
  a persistent override and uses to cancel the read timer. The same real socket
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
  `Upgrade: websocket` without `Connection: Upgrade`. The adapter raised
  `WebSockAdapter.UpgradeError` after routing, returned `500`, and
  `Ownership.owner_pid/1` was a live PID.
- Green: the router now runs
  `WebSockAdapter.UpgradeValidation.validate_upgrade/1` before
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
  moved` if Syn discards that owner. The real Bandit/WebSockex regression was red
  before owner monitoring and green afterward.

### Exercise ownership through real WebSockets

- Red: the black-box load client rejected `--scenario ownership`; it could only
  create many connections under one shared `serverId`, so it did not exercise
  the distributed ownership bottleneck.
- Green: the scenario opens one real v2 daemon-control WebSocket for every
  distinct `serverId`, in bounded batches, through the public `/ws` contract. A
  committed real-server test opens 1,000 distinct sessions. A manual local run
  opened 15,000 real WebSockets in 1.50 seconds with zero connection, send, or
  cleanup failures. Relay RSS was about 602–642 MB across repeated runs.
- Load-generator boundary: a single macOS source/destination tuple has 16,384
  ephemeral ports (`49152..65535`), so a 50,000-socket attempt failed in the
  client around 16.3k connections. The 50,000-owner three-node BEAM test and the
  15,000 real-socket test measure the two layers without misreporting client port
  exhaustion as relay failure.

### Shed overload using the listener's built-in ceiling

- Red: a real relay configured for one acceptor and two connections still
  upgraded all three requested WebSockets because the application ignored its
  listener admission settings.
- Green: the application maps generic listener settings to Thousand Island's
  existing per-acceptor `DynamicSupervisor.max_children` limit and bounded retry
  policy. The same real-network test upgrades exactly two sockets, rejects the
  excess connection, fails the load run, and increments
  `paseo_relay_connection_rejections_total` once.
- No application queue was added. A queued WebSocket handshake would retain the
  file descriptor and memory while hiding overload from the client. Bounded
  listener retries followed by connection shedding preserve an explicit retry
  boundary.

### Fail closed across owner and metrics races

- Red: after obtaining a real owner reservation, killing that owner before
  `WebSock.init/1` made `Owner.attach/3` exit with `:noproc`, crashing the socket
  process before it could install its owner monitor.
- Green: owner calls now translate owner death and call timeout into `:closed`.
  The same reserved-owner regression returns the existing
  `1012 Session expired` close path. Lookup-to-reserve and
  reservation-to-attach use the same bounded call boundary.
- Red: killing `PaseoRelay.Metrics` with `:kill` left its fixed telemetry handler
  registered. The replacement failed with `already_exists`, exhausted the
  application supervisor, and left the relay stopped.
- Green: metrics initialization reclaims the handler before attaching it and
  reuses the existing counter store. Fault injection now produces a new metrics
  PID while the original relay supervisor stays alive, `/metrics` returns 200,
  and counter values survive the restart.

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

- A real worst-case run held 15,000 distinct owner WebSockets with zero failures
  at about 642 MB relay RSS. The earlier 50,000-owner result exercised BEAM
  ownership without network sockets and therefore did not validate the 2 GB Fly
  Machine memory boundary.
- Fly now starts spare capacity at 10,000 connections and refuses new connections
  at 15,000. The generic listener is a final safety net at 20,000; operators with
  larger Machines can raise it explicitly. No production default exceeds the
  real-socket capacity run.

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
- The same seed-1 run also supplied a real maximum-frame red: Bandit's socket
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
