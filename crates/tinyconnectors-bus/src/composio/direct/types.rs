//! Payloads for the stateless direct reads and the network settings.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Which root certificates the module's HTTP client trusts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComposioTlsRoots {
    /// The roots bundled with the module (Mozilla's). The default, and what the
    /// module used before this setting existed.
    #[default]
    Bundled,
    /// The operating system's certificate store, so a corporate or
    /// TLS-inspecting CA the user's machine already trusts is trusted here too.
    Platform,
}

/// How the module's HTTP client reaches the network.
///
/// The host owns the policy (whether a proxy applies to this service at all,
/// which proxy, which hosts bypass it) and resolves it to this description; the
/// module applies it to the one destination it is about to call. An empty value
/// is the module's historical behaviour: no proxy, bundled roots.
///
/// Added in contract 1.10. Every field is optional on the wire, so a host that
/// omits the whole object, or a module that predates it, behaves as before.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComposioTransportConfig {
    /// Proxy to route requests through, e.g. `http://127.0.0.1:8080` or
    /// `socks5://proxy.internal:1080`. May carry credentials, so it is never
    /// logged and is redacted from `Debug`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    /// Destinations that bypass the proxy, in `NO_PROXY` form: `*`, an exact
    /// host, or a `.suffix` / `*.suffix`. Ignored when there is no proxy.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub no_proxy: Vec<String>,
    /// Which root store to trust.
    #[serde(default, skip_serializing_if = "is_bundled")]
    pub tls_roots: ComposioTlsRoots,
    /// Extra root certificates, PEM encoded, trusted in addition to
    /// [`tls_roots`](Self::tls_roots).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_ca_pem: Vec<String>,
}

// `skip_serializing_if` takes a path to `fn(&T) -> bool`.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_bundled(roots: &ComposioTlsRoots) -> bool {
    *roots == ComposioTlsRoots::Bundled
}

impl fmt::Debug for ComposioTransportConfig {
    /// Omits the proxy URL, which may embed a username and password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComposioTransportConfig")
            .field("proxy", &self.proxy_url.as_ref().map(|_| "<set>"))
            .field("no_proxy", &self.no_proxy)
            .field("tls_roots", &self.tls_roots)
            .field("extra_ca_pem", &self.extra_ca_pem.len())
            .finish()
    }
}

/// A Composio API key and where to use it, for one call.
///
/// Never stored: the module builds a transport from it, makes the call, and
/// drops it. Nothing here is logged or echoed back in a reply or an error.
#[derive(Clone, Serialize, Deserialize)]
pub struct ComposioDirectCredential {
    /// The Composio API key, sent as `x-api-key`.
    pub api_key: String,
    /// Composio entity the key acts as. Absent means `"default"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    /// Override for Composio's API base. It must be HTTPS or a loopback address;
    /// the module refuses anything else before a request is made. Absent means
    /// Composio's own API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Network settings for this call. Absent means no proxy and bundled roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<ComposioTransportConfig>,
}

impl fmt::Debug for ComposioDirectCredential {
    /// Omits the key.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComposioDirectCredential")
            .field("entity_id", &self.entity_id)
            .field("base_url", &self.base_url)
            .field("transport", &self.transport)
            .finish_non_exhaustive()
    }
}

/// Arguments for listing connections with a credential supplied per call.
///
/// Answers a [`crate::ComposioConnectionsResponse`]. Also what a host sends to
/// check an unsaved key: a key Composio rejects fails the call with Composio's
/// own message (`HTTP 401: Invalid API key`), and a key it accepts returns the
/// list. Nothing is persisted either way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposioDirectConnectionsRequest {
    /// The credential to read with.
    pub credential: ComposioDirectCredential,
}

/// Arguments for listing tools with a credential supplied per call.
///
/// Answers a [`crate::ComposioToolsResponse`] straight from Composio. Unlike
/// `ListTools`, the user's scope preference is not applied: that preference
/// belongs to the configured route's state, and this member reads none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposioDirectToolsRequest {
    /// The credential to read with.
    pub credential: ComposioDirectCredential,
    /// Toolkit slugs to list tools for. Empty means the whole tenant catalog.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub toolkits: Vec<String>,
    /// Composio action tags to filter by.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}
