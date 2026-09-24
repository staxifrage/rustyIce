use arc_swap::ArcSwap;
use rustyice_core::{config::Config, mount::MountRegistry, traits::AuthBackend};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Dependencies needed to apply a `Config` to the running server. Shared by
/// the SIGHUP reload path and the admin API config-edit endpoints.
pub struct ApplyDeps<'a> {
    pub config: &'a Arc<ArcSwap<Config>>,
    pub auth: &'a Arc<dyn AuthBackend + Send + Sync>,
    pub mounts: &'a MountRegistry,
    pub autodjs: &'a Arc<AutoDjRegistry>,
    pub relays: &'a Arc<RelayRegistry>,
    pub app_state: &'a crate::state::AppState,
    pub shutdown: &'a CancellationToken,
}

/// Outcome of a successful `apply_config` call.
#[derive(Debug, Default)]
pub struct ApplyOutcome {
    /// Human-readable notices about fields that were saved but require a
    /// process restart to take effect (e.g. `stream_bind`).
    pub warnings: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("auth reload: {0}")]
    Auth(String),
}

/// Watches for SIGHUP and reloads hot-reloadable config fields.
/// Runs until `shutdown` is cancelled.
///
/// `config_write_lock` is the same mutex held by API saves; SIGHUP acquires
/// it for its load+apply sequence so a half-written `config.toml` is never
/// observable to the reloader.
#[allow(clippy::too_many_arguments)]
pub async fn watch_sighup(
    config_path: PathBuf,
    config: Arc<ArcSwap<Config>>,
    auth: Arc<dyn AuthBackend + Send + Sync>,
    mounts: MountRegistry,
    autodjs: Arc<AutoDjRegistry>,
    relays: Arc<RelayRegistry>,
    app_state: crate::state::AppState,
    config_write_lock: Arc<tokio::sync::Mutex<()>>,
    shutdown: CancellationToken,
) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sighup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                error!("failed to register SIGHUP handler: {e}");
                return;
            }
        };

        loop {
            tokio::select! {
                _ = sighup.recv() => {
                    info!("SIGHUP received, reloading config from {}", config_path.display());
                    let _guard = config_write_lock.lock().await;
                    let deps = ApplyDeps {
                        config: &config,
                        auth: &auth,
                        mounts: &mounts,
                        autodjs: &autodjs,
                        relays: &relays,
                        app_state: &app_state,
                        shutdown: &shutdown,
                    };
                    do_reload_from_disk(&config_path, deps).await;
                }
                () = shutdown.cancelled() => {
                    info!("config reload task shutting down");
                    break;
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = config_write_lock; // silence unused on non-unix
        shutdown.cancelled().await;
    }
}

/// Apply `new_cfg` to the running server. Performs the same per-mount,
/// per-autodj, per-relay diff as the SIGHUP reload path. Restart-required
/// field changes are reported via [`ApplyOutcome::warnings`] instead of
/// being applied (the caller writes them to disk for a future restart to
/// pick up).
pub async fn apply_config(
    new_cfg: Config,
    deps: ApplyDeps<'_>,
) -> Result<ApplyOutcome, ApplyError> {
    let ApplyDeps { config, auth, mounts, autodjs, relays, app_state, shutdown } = deps;
    let mut outcome = ApplyOutcome::default();

    let old = config.load();

    if old.server.stream_bind != new_cfg.server.stream_bind {
        let w = "stream_bind changed — restart required for this to take effect".to_string();
        warn!("{w}");
        outcome.warnings.push(w);
    }
    if old.server.admin_bind != new_cfg.server.admin_bind {
        let w = "admin_bind changed — restart required for this to take effect".to_string();
        warn!("{w}");
        outcome.warnings.push(w);
    }
    if old.limits.ring_size != new_cfg.limits.ring_size {
        let w = "ring_size changed — restart required for this to take effect".to_string();
        warn!("{w}");
        outcome.warnings.push(w);
    }

    if let Err(e) = auth.reload(&new_cfg).await {
        error!("auth reload error: {e}");
        return Err(ApplyError::Auth(e.to_string()));
    }

    // Use `old_cfg` (loaded once) for both the surviving-metadata update
    // and the disappeared-mount removal below so they see a consistent
    // snapshot.
    let old_cfg = config.load_full();

    // Update metadata for mounts that survive in the new config, and
    // register entirely new mounts so the runtime registry matches the
    // configured set. New mounts use the configured ring/burst sizes and
    // start with the MP3 codec marker (the actual codec is set later when
    // a source connects — matches the bootstrap behavior in main.rs).
    for mount_cfg in &new_cfg.mounts {
        if let Some(mount) = mounts.get(&mount_cfg.path) {
            let old_info = mount.info.load_full();
            let new_info = rustyice_core::mount::MountInfo {
                path: old_info.path.clone(),
                codec: old_info.codec.clone(),
                source_password: mount_cfg.source_password.clone(),
                max_listeners: mount_cfg.max_listeners,
                metadata: rustyice_core::mount::MountMetadata {
                    name: mount_cfg.name.clone(),
                    description: mount_cfg.description.clone(),
                    genre: mount_cfg.genre.clone(),
                    url: mount_cfg.url.clone(),
                },
            };
            mount.info.store(Arc::new(new_info));
        } else {
            let bus = Arc::new(crate::bus::TokioBroadcastBus::new(
                new_cfg.limits.ring_size,
                mount_cfg.burst_size.unwrap_or(new_cfg.limits.burst_size) as usize,
            ));
            mounts.add(Arc::new(rustyice_core::mount::ActiveMount::new(
                rustyice_core::mount::MountInfo {
                    path: mount_cfg.path.clone(),
                    codec: rustyice_core::types::CodecId::MP3,
                    source_password: mount_cfg.source_password.clone(),
                    max_listeners: mount_cfg.max_listeners,
                    metadata: rustyice_core::mount::MountMetadata {
                        name: mount_cfg.name.clone(),
                        description: mount_cfg.description.clone(),
                        genre: mount_cfg.genre.clone(),
                        url: mount_cfg.url.clone(),
                    },
                },
                bus,
            )));
            info!(mount = %mount_cfg.path, "mount registered via reload");
        }
    }

    // Remove plain-mount entries that disappeared from `[[mounts]]`. Only
    // touches paths that were previously *configured* — dynamic mounts
    // (created on-the-fly by sources authenticating with the global
    // source_password) aren't in `old_cfg.mounts` so they're left alone,
    // and autodj/relay-owned paths live in their own sections and are
    // handled by those diffs below. Any active source/listeners on a
    // removed mount get disconnected; that matches the operator's intent
    // when they explicitly removed the mount from config.
    let new_mount_paths: std::collections::HashSet<&str> =
        new_cfg.mounts.iter().map(|m| m.path.as_str()).collect();
    for old in &old_cfg.mounts {
        if !new_mount_paths.contains(old.path.as_str()) {
            let _ = mounts.remove(&old.path);
            info!(mount = %old.path, "mount removed via reload");
        }
    }

    // ── AutoDJ diff ────────────────────────────────────────────────────────
    let new_paths: std::collections::HashSet<&str> =
        new_cfg.autodjs.iter().map(|a| a.mount.as_str()).collect();

    // Removed entries: cancel + drop mount.
    for old in &old_cfg.autodjs {
        if !new_paths.contains(old.mount.as_str()) {
            autodjs.cancel(&old.mount).await;
            let _ = mounts.remove(&old.mount);
            info!(mount = %old.mount, "autodj removed via SIGHUP");
        }
    }

    // New entries, and respawn-required changes.
    for new in &new_cfg.autodjs {
        let old = old_cfg.autodjs.iter().find(|a| a.mount == new.mount);
        let needs_respawn = match old {
            None => new.enabled,
            Some(o) => {
                o.enabled != new.enabled
                    || o.folder != new.folder
                    || o.loop_playlist != new.loop_playlist
                    || o.order != new.order
                    || o.transcode != new.transcode
                    || o.burst_size != new.burst_size
            }
        };

        if needs_respawn {
            autodjs.cancel(&new.mount).await;

            // Ensure mount exists; if not, register it now (new entry).
            if mounts.get(&new.mount).is_none() {
                let bus = Arc::new(crate::bus::TokioBroadcastBus::new(
                    new_cfg.limits.ring_size,
                    new.burst_size.unwrap_or(new_cfg.limits.burst_size) as usize,
                ));
                let codec_seed = match new.transcode.format {
                    rustyice_core::config::TranscodeFormat::Mp3 => {
                        rustyice_core::types::CodecId::MP3
                    }
                    rustyice_core::config::TranscodeFormat::Vorbis => {
                        rustyice_core::types::CodecId::VORBIS
                    }
                };
                mounts.add(Arc::new(rustyice_core::mount::ActiveMount::new(
                    rustyice_core::mount::MountInfo {
                        path: new.mount.clone(),
                        codec: codec_seed,
                        source_password: String::new(),
                        max_listeners: new.max_listeners,
                        metadata: rustyice_core::mount::MountMetadata {
                            name: new.name.clone(),
                            description: new.description.clone(),
                            genre: new.genre.clone(),
                            url: new.url.clone(),
                        },
                    },
                    bus,
                )));
            }

            if new.enabled {
                if let Some(mount) = mounts.get(&new.mount) {
                    let cancel = shutdown.child_token();
                    let player =
                        rustyice_autodj::AutoDjPlayer::from_config(new, mount, cancel.clone());
                    let handle = player.spawn();
                    autodjs.insert(new.clone(), cancel, handle).await;
                    info!(mount = %new.mount, "autodj spawned via SIGHUP");
                }
            } else {
                info!(mount = %new.mount, "autodj disabled via SIGHUP");
            }
        } else {
            // Metadata-only update: live-swap MountInfo.
            if let Some(mount) = mounts.get(&new.mount) {
                let old_info = mount.info.load_full();
                let new_info = rustyice_core::mount::MountInfo {
                    path: old_info.path.clone(),
                    codec: old_info.codec.clone(),
                    source_password: old_info.source_password.clone(),
                    max_listeners: new.max_listeners,
                    metadata: rustyice_core::mount::MountMetadata {
                        name: new.name.clone(),
                        description: new.description.clone(),
                        genre: new.genre.clone(),
                        url: new.url.clone(),
                    },
                };
                mount.info.store(Arc::new(new_info));
            }
        }
    }

    // ── Relay diff ─────────────────────────────────────────────────────────
    let new_relay_paths: std::collections::HashSet<&str> =
        new_cfg.relays.iter().map(|r| r.mount.as_str()).collect();

    for old in &old_cfg.relays {
        if !new_relay_paths.contains(old.mount.as_str()) {
            relays.cancel(&old.mount).await;
            let _ = mounts.remove(&old.mount);
            info!(mount = %old.mount, "relay removed via SIGHUP");
        }
    }

    for new in &new_cfg.relays {
        let old = old_cfg.relays.iter().find(|r| r.mount == new.mount);
        let needs_respawn = match old {
            None => new.enabled,
            Some(o) => {
                o.enabled != new.enabled
                    || o.upstream != new.upstream
                    || o.username != new.username
                    || o.password != new.password
                    || o.transcode != new.transcode
                    || o.burst_size != new.burst_size
            }
        };

        if needs_respawn {
            relays.cancel(&new.mount).await;

            if mounts.get(&new.mount).is_none() {
                let bus = Arc::new(crate::bus::TokioBroadcastBus::new(
                    new_cfg.limits.ring_size,
                    new.burst_size.unwrap_or(new_cfg.limits.burst_size) as usize,
                ));
                mounts.add(Arc::new(rustyice_core::mount::ActiveMount::new(
                    rustyice_core::mount::MountInfo {
                        path: new.mount.clone(),
                        codec: rustyice_core::types::CodecId::MP3,
                        source_password: String::new(),
                        max_listeners: new.max_listeners,
                        metadata: rustyice_core::mount::MountMetadata {
                            name: new.name.clone(),
                            description: new.description.clone(),
                            genre: new.genre.clone(),
                            url: new.url.clone(),
                        },
                    },
                    bus,
                )));
            }

            if new.enabled {
                if let Some(mount) = mounts.get(&new.mount) {
                    let cancel = shutdown.child_token();
                    let task = crate::relay::RelayTask::from_config(
                        new.clone(),
                        mount,
                        app_state.clone(),
                        cancel.clone(),
                    );
                    let handle = task.spawn();
                    relays.insert(new.clone(), cancel, handle).await;
                    info!(mount = %new.mount, "relay spawned via SIGHUP");
                }
            } else {
                info!(mount = %new.mount, "relay disabled via SIGHUP");
            }
        } else {
            // Metadata-only update: live-swap MountInfo.
            if let Some(mount) = mounts.get(&new.mount) {
                let old_info = mount.info.load_full();
                let new_info = rustyice_core::mount::MountInfo {
                    path: old_info.path.clone(),
                    codec: old_info.codec.clone(),
                    source_password: old_info.source_password.clone(),
                    max_listeners: new.max_listeners,
                    metadata: rustyice_core::mount::MountMetadata {
                        name: new.name.clone(),
                        description: new.description.clone(),
                        genre: new.genre.clone(),
                        url: new.url.clone(),
                    },
                };
                mount.info.store(Arc::new(new_info));
            }
        }
    }

    config.store(Arc::new(new_cfg));
    Ok(outcome)
}

/// SIGHUP reload helper: load the config file at `path` from disk, then
/// apply it via [`apply_config`]. Logs warnings and errors; does not return
/// them to the caller (the SIGHUP path is fire-and-forget).
async fn do_reload_from_disk(path: &std::path::Path, deps: ApplyDeps<'_>) {
    let new_cfg = match rustyice_core::config::load(path) {
        Ok(c) => c,
        Err(e) => {
            error!("config reload failed (keeping old config): {e}");
            return;
        }
    };
    match apply_config(new_cfg, deps).await {
        Ok(out) => {
            for w in &out.warnings {
                warn!("{w}");
            }
            info!("config reloaded successfully");
        }
        Err(e) => error!("config apply failed (keeping running config): {e}"),
    }
}

use rustyice_core::config::{AutoDjConfig, RelayConfig};
use std::collections::HashMap;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

/// Live set of running AutoDJ tasks, keyed by mount path. Shared between
/// `main` (initial spawn) and the SIGHUP reloader (diff + respawn).
#[derive(Default)]
pub struct AutoDjRegistry {
    inner: AsyncMutex<HashMap<String, AutoDjEntry>>,
}

struct AutoDjEntry {
    #[allow(dead_code)]
    cfg: AutoDjConfig,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl AutoDjRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(
        &self,
        cfg: AutoDjConfig,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    ) {
        let mut g = self.inner.lock().await;
        g.insert(cfg.mount.clone(), AutoDjEntry { cfg, cancel, handle });
    }

    pub async fn cancel(&self, mount: &str) {
        let entry = { self.inner.lock().await.remove(mount) };
        if let Some(e) = entry {
            e.cancel.cancel();
            let _ = e.handle.await;
        }
    }
}

/// Live set of running relay tasks, keyed by mount path. Shared between
/// `main` (initial spawn) and the SIGHUP reloader (diff + respawn).
#[derive(Default)]
pub struct RelayRegistry {
    inner: AsyncMutex<HashMap<String, RelayEntry>>,
}

struct RelayEntry {
    #[allow(dead_code)]
    cfg: RelayConfig,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl RelayRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(
        &self,
        cfg: RelayConfig,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    ) {
        let mut g = self.inner.lock().await;
        g.insert(cfg.mount.clone(), RelayEntry { cfg, cancel, handle });
    }

    pub async fn cancel(&self, mount: &str) {
        let entry = { self.inner.lock().await.remove(mount) };
        if let Some(e) = entry {
            e.cancel.cancel();
            let _ = e.handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyice_core::{
        config::{AuthConfig, LimitsConfig, LogFormat, LoggingConfig, MountConfig, ServerConfig},
        mount::{ActiveMount, MountInfo, MountMetadata},
        traits::BroadcastBus,
        types::{CodecId, StreamPacket},
    };
    use std::{pin::Pin, sync::Arc};

    struct NullBus;
    impl BroadcastBus for NullBus {
        fn publish(&self, _: Arc<StreamPacket>) {}
        fn subscribe(
            &self,
        ) -> Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send + 'static>> {
            Box::pin(futures::stream::empty())
        }
        fn subscriber_count(&self) -> usize {
            0
        }
    }

    fn base_config() -> Config {
        Config {
            server: ServerConfig {
                stream_bind: "0.0.0.0:8000".parse().unwrap(),
                admin_bind: "127.0.0.1:8001".parse().unwrap(),
                hostname: "localhost".to_string(),
            },
            logging: LoggingConfig { level: "info".to_string(), format: LogFormat::Json },
            auth: AuthConfig { users: vec![], source_password: None },
            limits: LimitsConfig {
                max_listeners_global: 500,
                ring_size: 64,
                slow_listener_grace_s: 2,
                source_max_kbps: None,
                burst_size: 65_536,
            },
            mounts: vec![MountConfig {
                path: "/stream".to_string(),
                source_password: "oldpw".to_string(),
                max_listeners: None,
                name: Some("Old Name".to_string()),
                description: None,
                genre: None,
                url: None,
                burst_size: None,
                transcode: None,
            }],
            tls: None,
            transcode: None,
            autodjs: vec![],
            relays: vec![],
        }
    }

    #[tokio::test]
    async fn reload_updates_mount_metadata() {
        let _config = Arc::new(ArcSwap::from_pointee(base_config()));
        let registry = MountRegistry::new();

        registry.add(Arc::new(ActiveMount::new(
            MountInfo {
                path: "/stream".to_string(),
                codec: CodecId::MP3,
                source_password: "oldpw".to_string(),
                max_listeners: None,
                metadata: MountMetadata {
                    name: Some("Old Name".to_string()),
                    ..Default::default()
                },
            },
            Arc::new(NullBus),
        )));

        let mut new_cfg = base_config();
        new_cfg.mounts[0].source_password = "newpw".to_string();
        new_cfg.mounts[0].name = Some("New Name".to_string());

        let mount = registry.get("/stream").unwrap();
        assert_eq!(mount.info.load().metadata.name.as_deref(), Some("Old Name"));

        let mount_cfg = &new_cfg.mounts[0];
        let old_info = mount.info.load_full();
        let new_info = MountInfo {
            path: old_info.path.clone(),
            codec: old_info.codec.clone(),
            source_password: mount_cfg.source_password.clone(),
            max_listeners: mount_cfg.max_listeners,
            metadata: MountMetadata {
                name: mount_cfg.name.clone(),
                description: mount_cfg.description.clone(),
                genre: mount_cfg.genre.clone(),
                url: mount_cfg.url.clone(),
            },
        };
        mount.info.store(Arc::new(new_info));

        assert_eq!(mount.info.load().source_password, "newpw");
        assert_eq!(mount.info.load().metadata.name.as_deref(), Some("New Name"));
    }

    #[tokio::test]
    async fn reload_preserves_current_title() {
        let _config = Arc::new(ArcSwap::from_pointee(base_config()));
        let registry = MountRegistry::new();

        registry.add(Arc::new(ActiveMount::new(
            MountInfo {
                path: "/stream".to_string(),
                codec: CodecId::MP3,
                source_password: "oldpw".to_string(),
                max_listeners: None,
                metadata: MountMetadata {
                    name: Some("Old Name".to_string()),
                    ..Default::default()
                },
            },
            Arc::new(NullBus),
        )));

        // Admin sets a runtime title before reload happens.
        let mount = registry.get("/stream").unwrap();
        mount.current_title.store(Arc::new(Some("Now Playing".to_string())));

        // Mimic the body of `do_reload` for the single mount: build a new
        // MountInfo and store it.
        let mut new_cfg = base_config();
        new_cfg.mounts[0].name = Some("Reloaded Name".to_string());
        let mount_cfg = &new_cfg.mounts[0];
        let old_info = mount.info.load_full();
        let new_info = MountInfo {
            path: old_info.path.clone(),
            codec: old_info.codec.clone(),
            source_password: mount_cfg.source_password.clone(),
            max_listeners: mount_cfg.max_listeners,
            metadata: MountMetadata {
                name: mount_cfg.name.clone(),
                description: mount_cfg.description.clone(),
                genre: mount_cfg.genre.clone(),
                url: mount_cfg.url.clone(),
            },
        };
        mount.info.store(Arc::new(new_info));

        // Title survives.
        let snap = mount.current_title.load();
        assert_eq!(snap.as_deref(), Some("Now Playing"));
        // Name was reloaded.
        assert_eq!(mount.info.load().metadata.name.as_deref(), Some("Reloaded Name"));
    }

    // ── apply_config tests ──────────────────────────────────────────────────

    use async_trait::async_trait;
    use rustyice_core::error::AuthError;
    use rustyice_core::traits::{IngestProtocol, OutputProtocol};
    use rustyice_core::types::{ListenerStats, SourceStats};

    struct OkAuth;
    #[async_trait]
    impl AuthBackend for OkAuth {
        async fn verify_admin(&self, _: &str, _: &str) -> Result<bool, AuthError> { Ok(false) }
        async fn verify_source(&self, _: &str, _: &str) -> Result<bool, AuthError> { Ok(false) }
        async fn reload(&self, _: &Config) -> Result<(), AuthError> { Ok(()) }
    }

    struct FailingAuth;
    #[async_trait]
    impl AuthBackend for FailingAuth {
        async fn verify_admin(&self, _: &str, _: &str) -> Result<bool, AuthError> { Ok(false) }
        async fn verify_source(&self, _: &str, _: &str) -> Result<bool, AuthError> { Ok(false) }
        async fn reload(&self, _: &Config) -> Result<(), AuthError> {
            Err(AuthError::ReloadFailed("simulated".into()))
        }
    }

    /// Test stub: never invoked because tests don't touch the relay branch.
    struct StubIngest;
    #[async_trait]
    impl IngestProtocol for StubIngest {
        fn name(&self) -> &'static str { "stub" }
        async fn run(
            &self,
            _reader: Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
            _bus: Arc<dyn BroadcastBus>,
            _codec: CodecId,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<SourceStats, rustyice_core::error::IngestError> {
            unreachable!("apply_config tests should never trigger ingest");
        }
    }

    struct StubOutput;
    #[async_trait]
    impl OutputProtocol for StubOutput {
        fn name(&self) -> &'static str { "stub" }
        async fn run(
            &self,
            _: Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
            _: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>>,
            _: Arc<MountInfo>,
            _: Arc<ArcSwap<Option<String>>>,
            _: Arc<ArcSwap<Option<rustyice_core::mount::SourceOverlay>>>,
            _: bool,
            _: &mut Option<Vec<u8>>,
            _: CancellationToken,
        ) -> Result<ListenerStats, rustyice_core::error::OutputError> {
            unreachable!("apply_config tests should never trigger output");
        }
    }

    fn make_app_state(
        config: Arc<ArcSwap<Config>>,
        auth: Arc<dyn AuthBackend + Send + Sync>,
        mounts: MountRegistry,
    ) -> crate::state::AppState {
        crate::state::AppState {
            mounts,
            auth,
            ingest: Arc::new(StubIngest),
            output: Arc::new(StubOutput),
            listeners: rustyice_admin::ListenerMap::new(),
            config,
            shutdown: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn apply_config_returns_stream_bind_warning() {
        let config = Arc::new(ArcSwap::from_pointee(base_config()));
        let auth: Arc<dyn AuthBackend + Send + Sync> = Arc::new(OkAuth);
        let mounts = MountRegistry::new();
        let autodjs = Arc::new(AutoDjRegistry::new());
        let relays = Arc::new(RelayRegistry::new());
        let app_state = make_app_state(config.clone(), auth.clone(), mounts.clone());
        let shutdown = CancellationToken::new();

        let mut new_cfg = base_config();
        new_cfg.server.stream_bind = "0.0.0.0:9000".parse().unwrap();

        let deps = ApplyDeps {
            config: &config,
            auth: &auth,
            mounts: &mounts,
            autodjs: &autodjs,
            relays: &relays,
            app_state: &app_state,
            shutdown: &shutdown,
        };
        let outcome = apply_config(new_cfg, deps).await.expect("apply succeeded");
        assert!(
            outcome.warnings.iter().any(|w| w.contains("stream_bind")),
            "expected stream_bind warning, got {:?}",
            outcome.warnings,
        );
        // New config was swapped in.
        assert_eq!(config.load().server.stream_bind.port(), 9000);
    }

    #[tokio::test]
    async fn apply_config_propagates_auth_error() {
        let config = Arc::new(ArcSwap::from_pointee(base_config()));
        let auth: Arc<dyn AuthBackend + Send + Sync> = Arc::new(FailingAuth);
        let mounts = MountRegistry::new();
        let autodjs = Arc::new(AutoDjRegistry::new());
        let relays = Arc::new(RelayRegistry::new());
        let app_state = make_app_state(config.clone(), auth.clone(), mounts.clone());
        let shutdown = CancellationToken::new();

        let deps = ApplyDeps {
            config: &config,
            auth: &auth,
            mounts: &mounts,
            autodjs: &autodjs,
            relays: &relays,
            app_state: &app_state,
            shutdown: &shutdown,
        };
        let err = apply_config(base_config(), deps).await.unwrap_err();
        assert!(matches!(err, ApplyError::Auth(_)));
    }

}
