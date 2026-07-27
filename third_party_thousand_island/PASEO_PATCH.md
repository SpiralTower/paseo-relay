# Paseo Thousand Island patch

This directory is Thousand Island 1.5.0, Hex outer checksum
`708923d40523e43cf99041ab37a0d4b0ec426ac6438fa3716ab23d919eaeb412`,
from upstream commit `62223053915edcc3beaa6ed8e43e3bfc1217fd7c`. Its only behavioral delta is the
generic `{:suspend, state}` handler continuation. Suspension leaves the socket
in passive mode while the connection process continues servicing ordinary OTP
messages. A later handler action may explicitly rearm the socket.

Paseo uses this with Bandit's suspended WebSocket callback result so a
backpressured full-duplex socket can still write frames without reading another
source packet. The extension contains no relay-specific behavior and is
intended to be upstreamable.

The only patched file is `lib/thousand_island/handler.ex`. Run
`scripts/verify-patched-dependencies.sh` from the relay repository to fetch and
hash pristine artifacts, reconstruct this directory, and run upstream tests.
To update or delete the patch, change/remove that file together with the path
dependency and verifier declarations; no external fork exists.
