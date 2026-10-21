//! Initial setup tests for a module with a host-issued storage lease.

use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tinybus::{
    Connection, InterfaceName, MemberName, ObjectPath,
    broker::Broker,
    module::{ModuleHost, PreReadyConfidentialRoute},
    transport::memory::{MemoryBus, MemoryTransport},
};
use tinystoragedrivers_busproxy::{
    AuditEvent, AuditSink, Environment, GovernedEndpointLimits, GovernedStorage, GrantPolicy,
    HostContext, HostFailure, ResolvedStorage, StorageGrant,
};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use tinystoragedrivers_tinybus::migration::{
    EXCHANGE_MEMBER, GET_SCHEMA_MEMBER, MIGRATION_INTERFACE, MIGRATION_OBJECT_PATH,
    MigrationModuleClient,
};

struct MigrationSetupContext {
    controller: Connection,
    module_peer: tinybus::BusName,
}

impl HostContext for MigrationSetupContext {}

struct MigrationSetupEnvironment {
    backend: Arc<MemoryStorage>,
    require_migration: Arc<AtomicBool>,
    migration_attempted: Arc<AtomicBool>,
    migration_discovered: Arc<AtomicBool>,
    resolves: Arc<AtomicUsize>,
}

#[async_trait]
impl Environment<MigrationSetupContext> for MigrationSetupEnvironment {
    async fn resolve(
        &self,
        context: &MigrationSetupContext,
        _module: &tinybus::module::ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        if self.require_migration.load(Ordering::Acquire) {
            self.migration_attempted.store(true, Ordering::Release);
            MigrationModuleClient::discover(&context.controller, &context.module_peer)
                .await
                .map_err(|_| HostFailure::Denied("connector-migration-unavailable"))?;
            self.migration_discovered.store(true, Ordering::Release);
        }
        let storage = self
            .backend
            .for_scope(&Scope::local())
            .map_err(|_| HostFailure::Denied("test-storage-unavailable"))?;
        let grant = StorageGrant::from_needs(
            (*tinyconnectors_bus::STORAGE_NEEDS).clone(),
            &GrantPolicy::default(),
        )
        .map_err(|_| HostFailure::Denied("connector-storage-grant-invalid"))?;
        Ok(ResolvedStorage {
            backend_identity: "connectors-initial-migration-test".to_owned(),
            binding_epoch: 1,
            canonical_scope: "connectors-initial-migration-test".to_owned(),
            grant,
            server: Arc::new(StorageServer::new(storage)),
        })
    }

    async fn admit_write(
        &self,
        _context: &MigrationSetupContext,
        _module: &tinybus::module::ModuleId,
    ) -> Result<(), HostFailure> {
        Ok(())
    }
}

struct MigrationSetupAudit;

impl AuditSink for MigrationSetupAudit {
    fn record(&self, _event: AuditEvent) {}
}

type MigrationSetupRegistration = tinystoragedrivers_tinybus::StorageRegistration<
    MigrationSetupEnvironment,
    MigrationSetupAudit,
    MigrationSetupContext,
>;

struct InitialSetupFixture {
    registration: Arc<MigrationSetupRegistration>,
    controller: Connection,
    require_migration: Arc<AtomicBool>,
}

static INITIAL_SETUP_FIXTURE: std::sync::OnceLock<std::sync::Mutex<Option<InitialSetupFixture>>> =
    std::sync::OnceLock::new();

fn initial_setup_fixture() -> &'static std::sync::Mutex<Option<InitialSetupFixture>> {
    INITIAL_SETUP_FIXTURE.get_or_init(|| std::sync::Mutex::new(None))
}

static INITIAL_SETUP_LEASE: std::sync::OnceLock<
    std::sync::Mutex<Option<tinystoragedrivers_busproxy::StorageLease>>,
> = std::sync::OnceLock::new();

fn initial_setup_lease()
-> &'static std::sync::Mutex<Option<tinystoragedrivers_busproxy::StorageLease>> {
    INITIAL_SETUP_LEASE.get_or_init(|| std::sync::Mutex::new(None))
}

mod initial_setup_export {
    use super::*;

    async fn setup(
        context: tinybus_module::ModuleSetupContext,
        _config: Option<ModuleConfig>,
    ) -> tinybus::Result<()> {
        let fixture = initial_setup_fixture()
            .lock()
            .map_err(|_| tinybus::Error::failed("initial setup fixture lock poisoned"))?
            .take()
            .ok_or_else(|| tinybus::Error::failed("initial setup fixture missing"))?;
        let connection = context.connection().clone();
        let peer = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("connector module has no unique peer"))?;
        let (lease, descriptor) = fixture
            .registration
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(MigrationSetupContext {
                    controller: fixture.controller,
                    module_peer: peer.clone(),
                }),
                peer,
            )
            .await
            .map_err(|_| tinybus::Error::failed("connector storage lease was refused"))?;

        // Lease registration performs the first resolve with migration probing
        // disabled. The configured production setup must then discover the
        // real module-owned migration service before resolving the bound route.
        fixture.require_migration.store(true, Ordering::Release);
        initial_setup_lease()
            .lock()
            .map_err(|_| tinybus::Error::failed("initial setup lease lock poisoned"))?
            .replace(lease);

        let config = ModuleConfig {
            storage_descriptor: Some(descriptor),
            ..ModuleConfig::default()
        };
        setup_with_context(context, config).await
    }

    tinybus_module::module_export_static! {
        setup_with_context = setup,
        config = Option<ModuleConfig>,
        worker_threads = 2,
        provides = [
            "ai.tinyhumans.connectors.Composio",
            "org.tinystoragedrivers.Migration",
        ],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_bound_storage_setup_discovers_connector_migrations()
-> Result<(), Box<dyn std::error::Error>> {
    let bus = MemoryBus::new();
    let broker = Broker::new_restricted_module_control();
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    broker.attach_module_controller(Arc::new(broker_transport))?;
    let controller = Connection::connect(Box::new(controller_transport)).await?;
    let broker_task = broker.spawn(bus.clone());
    let mut host = ModuleHost::new(broker);
    host.set_pre_ready_confidential_routes(vec![
        PreReadyConfidentialRoute {
            path: ObjectPath::new(MIGRATION_OBJECT_PATH)?,
            interface: InterfaceName::new(MIGRATION_INTERFACE)?,
            member: MemberName::new(GET_SCHEMA_MEMBER)?,
        },
        PreReadyConfidentialRoute {
            path: ObjectPath::new(MIGRATION_OBJECT_PATH)?,
            interface: InterfaceName::new(MIGRATION_INTERFACE)?,
            member: MemberName::new(EXCHANGE_MEMBER)?,
        },
    ])?;

    let backend = Arc::new(MemoryStorage::new());
    let require_migration = Arc::new(AtomicBool::new(false));
    let migration_attempted = Arc::new(AtomicBool::new(false));
    let migration_discovered = Arc::new(AtomicBool::new(false));
    let resolves = Arc::new(AtomicUsize::new(0));
    let registration: Arc<MigrationSetupRegistration> = Arc::new(
        tinystoragedrivers_tinybus::attach_storage(
            &host,
            &controller,
            GovernedStorage::new(
                Arc::new(MigrationSetupEnvironment {
                    backend,
                    require_migration: Arc::clone(&require_migration),
                    migration_attempted: Arc::clone(&migration_attempted),
                    migration_discovered: Arc::clone(&migration_discovered),
                    resolves: Arc::clone(&resolves),
                }),
                Arc::new(MigrationSetupAudit),
                GovernedEndpointLimits::default(),
            ),
            tokio::runtime::Handle::current(),
        )
        .await
        .map_err(|_| std::io::Error::other("governed storage attachment failed"))?,
    );
    *initial_setup_fixture()
        .lock()
        .map_err(|_| std::io::Error::other("initial setup fixture lock poisoned"))? =
        Some(InitialSetupFixture {
            registration: Arc::clone(&registration),
            controller: controller.clone(),
            require_migration: Arc::clone(&require_migration),
        });

    let info = host.attach_linked_with_config(
        initial_setup_export::linked_module()?,
        serde_json::json!({}),
    )?;
    if let Err(error) = host.ensure_ready(info.manifest.bus_name.as_str()).await {
        return Err(std::io::Error::other(format!(
            "initial connector setup failed (migration required: {}, discover attempted: {}, discover succeeded: {}): {error}",
            require_migration.load(Ordering::Acquire),
            migration_attempted.load(Ordering::Acquire),
            migration_discovered.load(Ordering::Acquire),
        ))
        .into());
    }
    assert!(
        migration_attempted.load(Ordering::Acquire),
        "initial bound storage validation must attempt GetSchema during setup"
    );
    assert!(
        migration_discovered.load(Ordering::Acquire),
        "initial bound storage validation must discover GetSchema during setup"
    );
    assert!(resolves.load(Ordering::SeqCst) >= 2);

    let lease = initial_setup_lease()
        .lock()
        .map_err(|_| std::io::Error::other("initial setup lease lock poisoned"))?
        .take()
        .ok_or_else(|| std::io::Error::other("initial setup did not retain its lease"))?;
    assert!(registration.revoke(&lease).await);
    let registration = Arc::try_unwrap(registration)
        .map_err(|_| std::io::Error::other("storage registration remained in use"))?;
    registration.shutdown().await?;
    host.shutdown(Duration::from_secs(2)).await;
    broker_task.abort();
    Ok(())
}
