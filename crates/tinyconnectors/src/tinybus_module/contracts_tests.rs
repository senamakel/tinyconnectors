//! The contract, probe and overview members against a loopback HTTP backend.
//!
//! Real sockets rather than a stubbed transport: the point is that the module
//! asks the backend the questions the host used to ask, and caches the answers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex, RwLock};

use serde_json::json;
use tinyconnectors_bus::composio::{
    ComposioConnectedIntegrationsMode, ComposioConnectedIntegrationsResponse,
};

use super::{ConnectorService, EphemeralPrefsStore, ModuleConfig};
use crate::client::test_support::{Reply, json as ok, server};
use crate::client::{ComposioClient, DirectRoute, HttpTransport, ProxyRoute, Route};

fn service_on(route: Arc<dyn Route>) -> ConnectorService {
    let client = Arc::new(RwLock::new(Some(ComposioClient::new(route))));
    ConnectorService {
        actions: Arc::new(crate::providers::ClientActions::new(Arc::clone(&client))),
        prefs: Arc::new(EphemeralPrefsStore::default()),
        registry: crate::providers::default_registry(),
        contract_caches: Arc::default(),
        client,
        storage: super::storage::StorageLink::none(),
        archives: Arc::default(),
        offered: tinyconnectors_bus::vocabulary::OfferedToolkits::default(),
    }
}

fn proxy_service(
    respond: impl Fn(&str) -> Reply + Send + Sync + 'static,
) -> (ConnectorService, Arc<Mutex<Vec<String>>>) {
    let (base, seen) = server(respond);
    let transport = Arc::new(HttpTransport::bearer(&base, "t0ken").unwrap());
    (service_on(Arc::new(ProxyRoute::new(transport))), seen)
}

fn direct_service(
    respond: impl Fn(&str) -> Reply + Send + Sync + 'static,
) -> (ConnectorService, Arc<Mutex<Vec<String>>>) {
    let (base, seen) = server(respond);
    let transport = Arc::new(HttpTransport::api_key(&base, "ck_key").unwrap());
    (
        service_on(Arc::new(DirectRoute::new(transport, "ck_key", "default"))),
        seen,
    )
}

fn request_path(head: &str) -> &str {
    head.lines()
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
}

fn count(seen: &Arc<Mutex<Vec<String>>>, needle: &str) -> usize {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|head| request_path(head).contains(needle))
        .count()
}

const GITHUB_TOOLS: &str = r#"{"tools":[
  {"type":"function","function":{"name":"GITHUB_LIST_REPOSITORY_ISSUES","description":"List issues",
   "parameters":{"type":"object","required":["owner","repo"]}}},
  {"type":"function","function":{"name":"GITHUB_SOMETHING_UNCURATED","description":"x",
   "output_parameters":{"properties":{"items":{"type":"array"}}}}}
]}"#;

#[tokio::test]
async fn a_contract_is_fetched_once_and_then_served_from_memory() {
    let (service, seen) = proxy_service(|head| {
        if request_path(head).contains("/tools") {
            ok(GITHUB_TOOLS)
        } else {
            ok("{}")
        }
    });

    let first = service
        .contract_with_probe("github_list_repository_issues")
        .await
        .unwrap()
        .expect("the action is published");
    assert_eq!(first.slug, "GITHUB_LIST_REPOSITORY_ISSUES");
    assert_eq!(first.required_args, ["owner", "repo"]);
    assert!(first.is_curated);

    let second = service
        .contract_with_probe("GITHUB_SOMETHING_UNCURATED")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.primary_array_path.as_deref(), Some("data.items"));
    assert!(!second.is_curated);

    assert_eq!(count(&seen, "/tools"), 1, "the second lookup hit the cache");
    assert!(
        seen.lock().unwrap()[0].contains("toolkits=github"),
        "the listing is scoped to the slug's toolkit and unfiltered by scope"
    );
}

#[tokio::test]
async fn an_unpublished_action_is_none_and_a_fetch_failure_is_an_error() {
    let (service, _) = proxy_service(|_| ok(GITHUB_TOOLS));
    assert!(
        service
            .contract_with_probe("GITHUB_NOT_PUBLISHED")
            .await
            .unwrap()
            .is_none()
    );
    assert!(service.contract_with_probe("").await.unwrap().is_none());

    let (broken, _) = proxy_service(|_| (500, Vec::new(), "boom".to_string()));
    assert!(broken.contract_with_probe("GITHUB_LIST_X").await.is_err());
}

#[tokio::test]
async fn reconfiguring_forgets_contracts_fetched_for_the_old_tenant() {
    let (service, _) = proxy_service(|_| ok(GITHUB_TOOLS));
    service.toolkit_contracts("github").await.unwrap();
    assert!(service.contract_caches.catalog("github").is_some());
    service
        .configure(tinyconnectors_bus::ComposioConfigureRequest::None)
        .await
        .unwrap();
    assert!(service.contract_caches.catalog("github").is_none());
}

#[tokio::test]
async fn concurrent_misses_share_one_fetch() {
    let (service, seen) = proxy_service(|_| ok(GITHUB_TOOLS));
    let service = Arc::new(service);
    let tasks: Vec<_> = (0..5)
        .map(|_| {
            let service = Arc::clone(&service);
            tokio::spawn(async move { service.toolkit_contracts("github").await.unwrap().1.len() })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), 2);
    }
    assert_eq!(count(&seen, "/tools"), 1);
}

fn connected_github(head: &str) -> Reply {
    let path = request_path(head);
    if path.contains("/connections") {
        ok(r#"{"connections":[{"id":"c1","toolkit":"github","status":"ACTIVE"}]}"#)
    } else if path.contains("/execute") {
        ok(r#"{"data":{"issues":[{"n":1}],"total":1},"successful":true,"error":null,"costUsd":0}"#)
    } else if path.contains("/tools") {
        ok(GITHUB_TOOLS)
    } else {
        ok("{}")
    }
}

#[tokio::test]
async fn a_probe_reports_the_real_shape_and_overlays_later_contracts() {
    let (service, seen) = proxy_service(connected_github);
    let sample = service
        .probe_output(
            "GITHUB_LIST_REPOSITORY_ISSUES",
            json!({"owner": "o", "repo": "r"}),
        )
        .await
        .unwrap();
    assert_eq!(sample.primary_array_path.as_deref(), Some("data.issues"));
    assert!(sample.output_fields.contains(&"issues".to_string()));
    assert_eq!(
        sample.sample["data"]["total"], 1,
        "the caller gets the raw sample"
    );
    assert_eq!(count(&seen, "/execute"), 1);

    // The contract now reflects the observation, which the schema never had.
    let contract = service
        .contract_with_probe("GITHUB_LIST_REPOSITORY_ISSUES")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(contract.primary_array_path.as_deref(), Some("data.issues"));
    assert!(contract.output_fields.contains(&"total".to_string()));

    // And the cache holds the shape, not the user's data.
    let cached = service
        .contract_caches
        .probe("GITHUB_LIST_REPOSITORY_ISSUES")
        .unwrap();
    assert_eq!(cached.sample, json!(null));
}

#[tokio::test]
async fn a_probe_refuses_anything_that_is_not_a_confirmed_read() {
    let (service, seen) = proxy_service(connected_github);
    for slug in ["GITHUB_CREATE_AN_ISSUE", "GITHUB_SOMETHING_UNCURATED", ""] {
        assert!(
            service.probe_output(slug, json!(null)).await.is_err(),
            "{slug}"
        );
    }
    assert_eq!(count(&seen, "/execute"), 0, "nothing was ever executed");
}

#[tokio::test]
async fn a_probe_refuses_a_toolkit_with_no_active_connection() {
    let (service, seen) = proxy_service(|head| {
        if request_path(head).contains("/connections") {
            ok(r#"{"connections":[{"id":"c1","toolkit":"github","status":"EXPIRED"}]}"#)
        } else {
            ok("{}")
        }
    });
    let error = service
        .probe_output("GITHUB_LIST_REPOSITORY_ISSUES", json!(null))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("no active connection"),
        "{error}"
    );
    assert_eq!(count(&seen, "/execute"), 0);
}

#[tokio::test]
async fn a_probe_reports_a_provider_refusal_with_its_detail() {
    let (service, _) = proxy_service(|head| {
        if request_path(head).contains("/execute") {
            ok(r#"{"data":{},"successful":false,"error":"rate limited","costUsd":0}"#)
        } else {
            connected_github(head)
        }
    });
    let error = service
        .probe_output("GITHUB_LIST_REPOSITORY_ISSUES", json!({}))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("rate limited"), "{error}");
}

#[tokio::test]
async fn a_probe_obeys_the_stored_scope_preference() {
    use tinyconnectors_sync::ScopePrefStore;
    let (service, seen) = proxy_service(connected_github);
    tinyconnectors_sync::UserScopePref {
        read: false,
        write: false,
        admin: false,
    }
    .save(service.prefs.as_ref(), "github")
    .await
    .unwrap();
    let error = service
        .probe_output("GITHUB_LIST_REPOSITORY_ISSUES", json!({}))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not permitted"), "{error}");
    assert_eq!(count(&seen, "/execute"), 0);
}

fn backend_overview(head: &str) -> Reply {
    let path = request_path(head);
    if path.contains("/toolkits") {
        ok(r#"{"toolkits":["gmail","github","slack"],"catalog":[
            {"slug":"gmail","enabled":true,"description":"Mail from the backend"},
            {"slug":"github","enabled":true},
            {"slug":"slack","enabled":false}]}"#)
    } else if path.contains("/connections") {
        ok(r#"{"connections":[
            {"id":"g1","toolkit":"gmail","status":"ACTIVE","accountEmail":"me@example.com"},
            {"id":"h1","toolkit":"github","status":"EXPIRED"}]}"#)
    } else if path.contains("/tools") {
        ok(r#"{"tools":[
            {"function":{"name":"GMAIL_FETCH_EMAILS","description":"Fetch"}},
            {"function":{"name":"GMAIL_DELETE_MESSAGE","description":"Delete"}}]}"#)
    } else {
        ok("{}")
    }
}

#[tokio::test]
async fn the_backend_overview_follows_the_catalog_connections_and_preferences() {
    let (service, seen) = proxy_service(backend_overview);
    let overview = service
        .connected_integrations(ComposioConnectedIntegrationsMode::Backend)
        .await
        .unwrap();

    let slugs: Vec<_> = overview.iter().map(|i| i.toolkit.as_str()).collect();
    assert_eq!(
        slugs,
        ["github", "gmail"],
        "the disabled slack entry is dropped"
    );
    let gmail = &overview[1];
    assert!(gmail.connected);
    assert_eq!(gmail.description, "Mail from the backend");
    assert_eq!(
        gmail.connections[0].label.as_deref(),
        Some("me@example.com")
    );
    assert_eq!(gmail.tools.len(), 1);
    assert_eq!(gmail.gated_tools[0].name, "GMAIL_DELETE_MESSAGE");
    assert_eq!(overview[0].non_active_status.as_deref(), Some("EXPIRED"));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|head| request_path(head).contains("toolkits=gmail")),
        "tools are listed for the connected subset only"
    );

    // The wire shape the host decodes.
    let wire = serde_json::to_value(ComposioConnectedIntegrationsResponse {
        integrations: overview,
    })
    .unwrap();
    assert_eq!(
        wire["integrations"][1]["gated_tools"][0]["required_scope"],
        "admin"
    );
}

#[tokio::test]
async fn an_empty_backend_allowlist_is_an_empty_overview() {
    let (service, seen) = proxy_service(|_| ok(r#"{"toolkits":[],"catalog":[]}"#));
    let overview = service
        .connected_integrations(ComposioConnectedIntegrationsMode::Backend)
        .await
        .unwrap();
    assert_eq!(overview, Vec::new());
    assert_eq!(count(&seen, "/connections"), 0);
}

#[tokio::test]
async fn a_backend_failure_is_an_error_so_the_host_does_not_cache_a_degraded_overview() {
    for failing in ["/toolkits", "/connections", "/tools"] {
        let (service, _) = proxy_service(move |head| {
            if request_path(head).contains(failing) {
                (500, Vec::new(), "boom".to_string())
            } else {
                backend_overview(head)
            }
        });
        assert!(
            service
                .connected_integrations(ComposioConnectedIntegrationsMode::Backend)
                .await
                .is_err(),
            "{failing}"
        );
    }
}

fn direct_overview(head: &str) -> Reply {
    let path = request_path(head);
    if path.contains("/connected_accounts") {
        ok(r#"{"items":[
            {"id":"d1","toolkit":"gmail","status":"ACTIVE"},
            {"id":"d2","toolkit":"notion","status":"ACTIVE"},
            {"id":"d3","toolkit":"slack","status":"INITIATED"}]}"#)
    } else if path.contains("/tools") {
        ok(r#"{"items":[{"slug":"GMAIL_FETCH_EMAILS","description":"Fetch"}]}"#)
    } else {
        ok("{}")
    }
}

#[tokio::test]
async fn the_direct_overview_uses_the_active_connections_as_the_allowlist() {
    let (service, _) = direct_service(direct_overview);
    let overview = service
        .connected_integrations(ComposioConnectedIntegrationsMode::Direct)
        .await
        .unwrap();
    let slugs: Vec<_> = overview.iter().map(|i| i.toolkit.as_str()).collect();
    assert_eq!(slugs, ["gmail", "notion"], "inactive slack is not listed");
    assert_eq!(overview[0].tools[0].name, "GMAIL_FETCH_EMAILS");
    assert_eq!(overview[1].tools, Vec::new());
    assert_eq!(
        overview[0].description,
        tinyconnectors_sync::toolkits::catalog::toolkit_description("gmail"),
        "direct mode has no catalog, so the compiled table describes it"
    );
}

#[tokio::test]
async fn a_direct_tool_listing_failure_only_costs_the_tools() {
    let (service, _) = direct_service(|head| {
        if request_path(head).contains("/tools") {
            (500, Vec::new(), "boom".to_string())
        } else {
            direct_overview(head)
        }
    });
    let overview = service
        .connected_integrations(ComposioConnectedIntegrationsMode::Direct)
        .await
        .unwrap();
    assert_eq!(overview.len(), 2);
    assert!(overview.iter().all(|i| i.connected && i.tools.is_empty()));
}

#[tokio::test]
async fn a_direct_tenant_with_no_active_connections_is_an_empty_overview() {
    let (service, _) = direct_service(|_| ok(r#"{"items":[]}"#));
    assert_eq!(
        service
            .connected_integrations(ComposioConnectedIntegrationsMode::Direct)
            .await
            .unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn a_mode_that_disagrees_with_the_live_route_is_refused() {
    let (proxy, seen) = proxy_service(backend_overview);
    let error = proxy
        .connected_integrations(ComposioConnectedIntegrationsMode::Direct)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("configured for `proxy`"),
        "{error}"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "no request left the module"
    );

    let (direct, _) = direct_service(direct_overview);
    assert!(
        direct
            .connected_integrations(ComposioConnectedIntegrationsMode::Backend)
            .await
            .is_err()
    );
}

async fn unconfigured_proxy() -> tinybus::Result<(tinybus::Connection, tinybus::Proxy)> {
    use tinybus::broker::Broker;
    use tinybus::transport::memory::MemoryBus;
    use tinyconnectors_bus::names;

    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let serving = tinybus::Connection::connect(bus.connect().await?).await?;
    super::setup(serving.clone(), ModuleConfig::default()).await?;
    let client = tinybus::Connection::connect(bus.connect().await?).await?;
    let proxy = client.proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)?;
    Ok((serving, proxy))
}

#[tokio::test]
async fn the_new_members_are_served_over_the_bus_by_their_contract_names() -> tinybus::Result<()> {
    use tinyconnectors_bus::composio::{
        ComposioGetToolContractRequest, ComposioGetToolContractResponse,
        ComposioListConnectedIntegrationsRequest, ComposioListToolContractsRequest,
        ComposioProbeToolOutputRequest,
    };
    use tinyconnectors_bus::names::methods;

    let (_serving, proxy) = unconfigured_proxy().await?;

    // A slug with no toolkit needs no route: the answer is "no such contract".
    let none: ComposioGetToolContractResponse = proxy
        .call(
            methods::GET_TOOL_CONTRACT,
            (ComposioGetToolContractRequest {
                slug: String::new(),
            },),
        )
        .await?;
    assert!(none.contract.is_none());

    // Everything that needs the backend says what is missing.
    let listing = proxy
        .call::<serde_json::Value>(
            methods::LIST_TOOL_CONTRACTS,
            (ComposioListToolContractsRequest {
                toolkit: "github".into(),
            },),
        )
        .await;
    assert!(
        listing
            .unwrap_err()
            .to_string()
            .contains("without a connector route")
    );

    let overview = proxy
        .call::<serde_json::Value>(
            methods::LIST_CONNECTED_INTEGRATIONS,
            (ComposioListConnectedIntegrationsRequest {
                mode: ComposioConnectedIntegrationsMode::Backend,
            },),
        )
        .await;
    assert!(
        overview
            .unwrap_err()
            .to_string()
            .contains("without a connector route")
    );

    // A probe refuses on scope before it needs a route at all.
    let probe = proxy
        .call::<serde_json::Value>(
            methods::PROBE_TOOL_OUTPUT,
            (ComposioProbeToolOutputRequest {
                slug: "GITHUB_CREATE_AN_ISSUE".into(),
                args: json!({}),
            },),
        )
        .await;
    assert!(probe.unwrap_err().to_string().contains("probe refuses"));
    Ok(())
}
