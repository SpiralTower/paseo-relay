# Paseo Bandit patch

This directory is Bandit 1.12.3, Hex outer checksum
`a253ec03f391755b2126e4181ee2fee05c75b712b407aa399de1831b5088c58e`,
from upstream commit `6e2ab9cce4869759809da0a2bce6e6e081cf80ae`. It is pinned locally because strict relay ingress
admission must run after a WebSocket frame header is validated but before its
payload is read.

The behavioral deltas from upstream 1.12.3 are the generic
`before_payload(length)` extractor option, tracked payload/message-completion deadlines
that remain fixed across fragmented-message assembly,
and suspended/resumed WebSocket or
payload-admission results. Suspension preserves extractor state without asking
the transport to read again; an admission or delivery message resumes buffered
extraction and rearms the socket. These extensions contain no Paseo code and
are intended to be upstreamable. `PaseoRelay.Delivery.Budget` and
`PaseoRelay.Socket` own all relay-specific behavior.

The only patched files are `lib/bandit.ex`, `lib/bandit/extractor.ex`,
`lib/bandit/websocket/connection.ex`, and `lib/bandit/websocket/handler.ex`.
Run `scripts/verify-patched-dependencies.sh` from the relay repository to fetch
and hash pristine artifacts, reconstruct this directory, and run upstream
tests. To update or delete the patch, change/remove those four files together
with the path dependency and verifier declarations; no external fork exists.
