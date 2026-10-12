# Domune connection hooks

This small patch starts from upstream golib v0.8.2, commit
`49e9f4533c54cd49db878534659b3423dcf54f69`, and changes only the existing dial
pipeline. It does not introduce or copy a proxy implementation.

`WithDialer(func(context.Context, string) (net.Conn, error))` exposes the existing
raw dialer slot. The address is the configured proxy when present, otherwise the
target. A custom dialer must honor its context. Existing proxy negotiation and
before/after hooks still run normally.

`DialContext` applies the configured timeout to the complete dial and handshake
phase. After raw dialing, a context cancellation callback closes the socket to
interrupt blocking proxy/transport I/O even when an upstream hook does not
consult the context. The callback is stopped and joined before return; subsequent
cancellation cannot close an established stream. No temporary socket deadlines
are installed or left behind. Cancellation returns the relevant context error.
Lazy TLS wrapping remains unchanged; clients that require eager validation add a
handshake hook to the existing pipeline.

Verification: `go test ./net -race -count=1 -timeout=30s`, with isolated loopback
fixtures for HTTP CONNECT and SOCKS5 authentication/target bytes, binary echo,
stalled handshakes under cancellation/context deadline/dial timeout, server-side
socket release, successful stream survival past its dial deadline, raw dialer
injection, and a failing hook that discards its context. Each dial and fixture
goroutine is observed to finish in the cancellation tests. No deployment,
platform matrix or unrelated package tests are required for this patch.
