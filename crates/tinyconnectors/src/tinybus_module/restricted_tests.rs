//! Restricted initialization, dispatch refusals, and immutable runtime handshake.

use super::*;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinyconnectors_bus::names::methods;

async fn fixture() -> (Connection, Connection, tinybus::Proxy) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let serving = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup(serving.clone()).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();
    (serving, caller, proxy)
}

#[tokio::test]
async fn restricted_instance_denies_stateful_calls_before_decoding_payloads() {
    let temporary = Temporary::new();
    let mut permissions = std::fs::metadata(temporary.path()).unwrap().permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(temporary.path(), permissions).unwrap();
    let (_serving, _caller, proxy) = fixture().await;
    let mode: ComposioRuntimeModeResponse = proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    assert_eq!(mode.mode, ComposioRuntimeMode::RoutedReadOnly);
    assert_eq!(mode.contract_version, tinyconnectors_bus::CONTRACT_VERSION);
    for member in tinyconnectors_bus::METHODS {
        if [
            methods::RUNTIME_MODE,
            methods::ROUTED_READ,
            methods::LIST_CAPABILITIES,
            methods::LIST_AGENT_READY_TOOLKITS,
        ]
        .contains(member)
        {
            continue;
        }
        let error = proxy.call::<serde_json::Value>(member, serde_json::json!({
            "route":"proxy", "auth_token":"private-token", "base_url":"https://example.com",
            "state_dir":temporary.path(), "archive_dir":temporary.path(), "directory":temporary.path()
        })).await.unwrap_err();
        assert_eq!(
            error.wire_name(),
            tinybus::Error::UNKNOWN_METHOD,
            "{member}"
        );
        assert!(!error.to_string().contains("private-token"));
    }
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn restricted_instance_keeps_confidential_delivery_mandatory() {
    let (_serving, _caller, proxy) = fixture().await;
    let error = proxy
        .call::<serde_json::Value>(methods::ROUTED_READ, "malformed-secret")
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), tinybus::Error::CONFIDENTIALITY_REQUIRED);
    assert!(!error.to_string().contains("malformed-secret"));
}

#[tokio::test]
async fn restricted_static_capabilities_need_no_route_or_state() {
    let (_serving, _caller, proxy) = fixture().await;
    let capabilities: ComposioCapabilitiesResponse =
        proxy.call(methods::LIST_CAPABILITIES, ()).await.unwrap();
    let ready: ComposioAgentReadyToolkitsResponse = proxy
        .call(methods::LIST_AGENT_READY_TOOLKITS, ())
        .await
        .unwrap();
    assert!(
        capabilities.capabilities.is_empty(),
        "restricted mode has no native provider or execution capabilities"
    );
    assert_ne!(ready.toolkits, Vec::new());
}

#[cfg(feature = "static-link")]
#[tokio::test(flavor = "current_thread")]
async fn compiled_restricted_module_attests_and_performs_confidential_read_without_state() {
    use crate::client::test_support::{json, server};
    let (base, seen) = server(|_| {
        json(r#"{"connections":[{"id":"routed","toolkit":"gmail","status":"ACTIVE"}]}"#)
    });
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let host = tinybus::module::ModuleHost::new(broker);
    host.attach_linked_with_config(
        crate::tinybus_module::linked_module().unwrap(),
        serde_json::json!({"runtime_mode":"routed_read_only"}),
    )
    .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if proxy.attestation().await.unwrap().is_some()
                && host
                    .list()
                    .iter()
                    .any(|module| matches!(module.state, tinybus::module::ModuleState::Ready))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let reply: ComposioRoutedReadReply = proxy
        .call_confidential(
            methods::ROUTED_READ,
            (ComposioRoutedReadRequest {
                route: tinyconnectors_bus::ComposioCallRoute::Proxy {
                    base_url: base,
                    auth_token: "fake-loopback-token".into(),
                    timezone: None,
                    transport: None,
                },
                operation: tinyconnectors_bus::ComposioReadOperation::ListConnections,
            },),
        )
        .await
        .unwrap();
    let ComposioRoutedReadReply::ListConnections(reply) = reply else {
        panic!("wrong reply")
    };
    assert_eq!(reply.connections[0].id, "routed");
    assert!(seen.lock().unwrap()[0].contains("fake-loopback-token"));
    let mode: ComposioRuntimeModeResponse = proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    assert_eq!(mode.mode, ComposioRuntimeMode::RoutedReadOnly);
    assert_eq!(mode.contract_version, tinyconnectors_bus::CONTRACT_VERSION);
    let attempted = host
        .reinitialize("tinyconnectors", serde_json::json!({}))
        .await;
    assert!(
        attempted.is_err(),
        "a restricted instance must never become classic"
    );
    let mode: ComposioRuntimeModeResponse = proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    assert_eq!(mode.mode, ComposioRuntimeMode::RoutedReadOnly);
}

struct Temporary(std::path::PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "tinyconnectors-restricted-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        Self(directory)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(feature = "static-link")]
async fn compiled_instance(
    config: serde_json::Value,
) -> (tinybus::module::ModuleHost, Connection, tinybus::Proxy) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let host = tinybus::module::ModuleHost::new(broker);
    host.attach_linked_with_config(crate::tinybus_module::linked_module().unwrap(), config)
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if proxy.attestation().await.unwrap().is_some()
                && host
                    .list()
                    .iter()
                    .any(|module| matches!(module.state, tinybus::module::ModuleState::Ready))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (host, caller, proxy)
}
#[cfg(feature = "static-link")]
#[tokio::test(flavor = "current_thread")]
async fn compiled_modes_are_per_instance_and_same_mode_reinitialization_remains_compatible() {
    let (
        (classic, _classic_caller, classic_proxy),
        (restricted, _restricted_caller, restricted_proxy),
    ) = tokio::join!(
        compiled_instance(serde_json::json!({})),
        compiled_instance(serde_json::json!({"runtime_mode":"routed_read_only"}))
    );
    let (classic_result, restricted_result) = tokio::join!(
        classic.reinitialize("tinyconnectors", serde_json::json!({})),
        restricted.reinitialize(
            "tinyconnectors",
            serde_json::json!({"runtime_mode":"routed_read_only"})
        )
    );
    classic_result.unwrap();
    restricted_result.unwrap();
    let classic_mode: ComposioRuntimeModeResponse =
        classic_proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    let restricted_mode: ComposioRuntimeModeResponse = restricted_proxy
        .call(methods::RUNTIME_MODE, ())
        .await
        .unwrap();
    assert_eq!(classic_mode.mode, ComposioRuntimeMode::Classic);
    assert_eq!(restricted_mode.mode, ComposioRuntimeMode::RoutedReadOnly);
    assert!(
        classic
            .reinitialize(
                "tinyconnectors",
                serde_json::json!({"runtime_mode":"routed_read_only"})
            )
            .await
            .is_err(),
        "an admitted classic instance must keep its original mode"
    );
    let classic_mode: ComposioRuntimeModeResponse =
        classic_proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    assert_eq!(classic_mode.mode, ComposioRuntimeMode::Classic);
    let capabilities: ComposioCapabilitiesResponse = classic_proxy
        .call(methods::LIST_CAPABILITIES, ())
        .await
        .unwrap();
    assert!(
        !capabilities.capabilities.is_empty(),
        "legacy capability behavior remains available"
    );
}
#[cfg(feature = "static-link")]
#[tokio::test(flavor = "current_thread")]
async fn compiled_restricted_instance_refuses_mode_change_before_opening_state() {
    let directory = Temporary::new();
    let (host, _caller, proxy) =
        compiled_instance(serde_json::json!({"runtime_mode":"routed_read_only"})).await;
    assert!(
        host.reinitialize(
            "tinyconnectors",
            serde_json::json!({
                "route":"proxy", "base_url":"http://127.0.0.1:1", "auth_token":"fake-token",
                "state_dir":directory.path()
            })
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        0,
        "refused reinitialization cannot create a preferences or archive directory"
    );
    let mode: ComposioRuntimeModeResponse = proxy.call(methods::RUNTIME_MODE, ()).await.unwrap();
    assert_eq!(mode.mode, ComposioRuntimeMode::RoutedReadOnly);
    let error = proxy
        .call::<serde_json::Value>(
            methods::CONFIGURE,
            serde_json::json!({
                "route":"proxy","base_url":"http://127.0.0.1:1","auth_token":"fake-token",
                "state_dir":directory.path()
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), tinybus::Error::UNKNOWN_METHOD);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
