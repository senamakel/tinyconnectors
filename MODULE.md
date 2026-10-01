# TinyConnectors TinyBus Module

This package contains the native `tinyconnectors` module for TinyBus module ABI
v1. Install only the archive matching the host operating system and
architecture.

The module claims `ai.tinyhumans.connectors.Composio`, serves the object at
`/ai/tinyhumans/connectors/Composio`, and provides `ListToolkits`,
`ListConnections`, `Authorize`, and `DeleteConnection`. Every payload type, the
interface name, the object path, and the member names are published as the
`tinyconnectors-bus` crate, so a host names them from a library rather than by
string literal.

## Configuration

The module requires a JSON configuration blob at load time, tagged by the route
it should use:

```json
{ "route": "proxy",  "base_url": "https://api.example.com", "auth_token": "<user session token>" }
{ "route": "direct", "api_key": "<user Composio key>", "entity_id": "default" }
```

**proxy** goes through the TinyHumans backend, which owns the Composio API key,
the billing margin, the toolkit allowlist, and the HMAC verification of inbound
webhooks. **direct** goes straight to `backend.composio.dev/api/v3` with the
user's own key.

**proxy** also takes an optional `"timezone"` (an IANA name such as
`"Asia/Kolkata"`, contract 1.9), sent to the backend as an `x-timezone` header
so it can render upstream UTC timestamps in the user's local time. Without it
the backend renders UTC. Only IANA-shaped names are forwarded.

Both routes also take an optional `"transport"` object (contract 1.10) carrying
the host's network policy, already resolved for this service:

```json
{ "proxy_url": "http://127.0.0.1:8080", "no_proxy": ["localhost", ".internal"], "tls_roots": "platform" }
```

`proxy_url` (`http`, `https`, `socks4`, `socks4a`, `socks5`, `socks5h`) routes
every request through that proxy, except to a destination matching `no_proxy`
(`*`, a domain and its subdomains, an IP, or a CIDR range). `tls_roots` is
`"bundled"` (default) or `"platform"` (the operating system's certificate
store). Without a proxy the process environment applies, as before. The direct
route never follows a redirect, because its `x-api-key` header would follow it.
A proxy URL that cannot be used fails the configuration rather than being
ignored, and the failure never quotes the URL, which may embed credentials.

The module implements both routes and selects neither — which one to use depends
on whether the user is signed in and whether they supplied a key, and those are
the host's decisions. Change route by reloading the module with a different
blob.

The credential is the host's to supply either way. The module never reads one
from the environment, never logs it, and never returns it through a member. It
also refuses a `base_url` that is not HTTPS or a genuine loopback address, so a
misconfiguration cannot send the credential somewhere it should not go. Loading
without the credential its route needs fails, rather than producing a module
that answers every call with a 401.

### Reading as a credential that is not the configured one

The module holds one configured route, so a host serving several credentials
(or checking one it has not saved) cannot share it. `ListConnectionsDirect` and
`ListToolsDirect` (contract 1.10) take the credential in the request itself:
`api_key`, and optionally `entity_id`, `base_url` and `transport`. They use and
replace nothing, keep nothing after the call, and apply no user scope
preference. A failure is the message the user reads, for example
`Composio v3 connected_accounts failed: HTTP 401: Invalid API key`, which is how
a host tells a rejected key from an outage. The key is never logged or returned.

### The routes are not equivalent

Direct mode cannot answer `ListToolkits` — there is no per-user allowlist when
you talk to Composio directly — or `DeleteConnection`, whose proxy version also
clears memory sourced from the connection. Both return a named refusal rather
than an empty result that would read like an answer.

## Installing

The archive contains one `.so`, `.dylib`, or `.dll` plus `modules.toml`. Keep
those files together when copying them into a TinyBus module directory. The
allowlist binds the native library filename to its SHA-256 digest so TinyBus can
reject a missing, renamed, or modified artifact before initialization.

The GitHub release also publishes `checksum.toml` as a separate asset. TinyBus
checks that manifest before downloading and extracting the selected platform
archive. Install directly from a tagged release with:

```sh
tinybus modules load-github \
  https://github.com/tinyhumansai/tinyconnectors/releases/tag/v0.1.5 \
  tinyconnectors-0.1.5-ubuntu-24.04-x86_64.tar.gz \
  <archive-sha256>
```

TinyBus modules are trusted in-process code. Install release artifacts only
from a trusted source and restart the host after replacing a loaded module.
