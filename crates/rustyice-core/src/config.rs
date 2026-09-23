use std::net::SocketAddr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub auth: AuthConfig,
    pub limits: LimitsConfig,
    #[serde(default)]
    pub mounts: Vec<MountConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub autodjs: Vec<AutoDjConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relays: Vec<RelayConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcode: Option<TranscodeConfig>,
}

impl Config {
    /// Returns the effective transcode config for `mount`: per-mount takes precedence over global.
    #[must_use]
    pub fn effective_transcode<'a>(&'a self, mount: &'a MountConfig) -> Option<&'a TranscodeConfig> {
        mount.transcode.as_ref().or(self.transcode.as_ref())
    }

    /// Returns the effective burst-on-connect size in bytes for `mount`:
    /// per-mount takes precedence over the global default in `[limits]`.
    #[must_use]
    pub fn effective_burst_size(&self, mount: &MountConfig) -> u32 {
        mount.burst_size.unwrap_or(self.limits.burst_size)
    }

    /// Verify that every `[[mounts]].path` and `[[autodjs]].mount` is unique.
    /// Call after parsing.
    ///
    /// # Errors
    /// Returns a human-readable description of the first collision found.
    pub fn validate_paths(&self) -> Result<(), String> {
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for m in &self.mounts {
            if !seen.insert(&m.path) {
                return Err(format!("duplicate mount path: {}", m.path));
            }
        }
        for a in &self.autodjs {
            if !seen.insert(&a.mount) {
                return Err(format!("duplicate autodj mount path: {}", a.mount));
            }
        }
        for r in &self.relays {
            if !seen.insert(&r.mount) {
                return Err(format!("duplicate relay mount path: {}", r.mount));
            }
            if !(r.upstream.starts_with("http://") || r.upstream.starts_with("https://")) {
                return Err(format!(
                    "relay '{}' upstream must be an http:// or https:// URL, got '{}'",
                    r.mount, r.upstream,
                ));
            }
            match (&r.username, &r.password) {
                (Some(_), None) => {
                    return Err(format!("relay '{}' has username but no password", r.mount));
                }
                (None, Some(_)) => {
                    return Err(format!("relay '{}' has password but no username", r.mount));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn default_burst_size() -> u32 {
    65_536
}

fn default_icy_metaint() -> u32 {
    16_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub stream_bind: SocketAddr,
    pub admin_bind: SocketAddr,
    pub hostname: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub users: Vec<UserConfig>,
    /// If set, any source authenticating with this password may create a new
    /// mount on a path not present in `[[mounts]]`. The mount lives for the
    /// duration of the source connection and is removed when the source
    /// disconnects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_password: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    /// Full access: can edit server, transcode, users, plus all stream-y
    /// sections (mounts, autodjs, relays).
    Admin,
    /// Stream operator: limited to mounts, autodjs, and relays. Cannot
    /// change server settings or add/remove other users.
    Operator,
}

fn default_user_role() -> UserRole {
    // Backward compatibility: configs written before roles existed only
    // contained admin-level accounts. Treat unspecified role as Admin so
    // existing operators don't suddenly lose access on upgrade.
    UserRole::Admin
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserConfig {
    pub username: String,
    pub password_bcrypt: String,
    #[serde(default = "default_user_role")]
    pub role: UserRole,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LimitsConfig {
    pub max_listeners_global: u32,
    pub ring_size: usize,
    pub slow_listener_grace_s: u64,
    /// Cap source ingestion rate (kbits/sec). Unset = unlimited.
    /// Useful when pushing files: limits reads so TCP backpressure slows the
    /// sender to real-time. Live source clients (Liquidsoap, Butt) send at
    /// their own bitrate and are unaffected as long as they stay below the cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_max_kbps: Option<u32>,
    /// Per-listener burst-on-connect size in bytes. New listeners receive up
    /// to this many recent stream bytes before transitioning to live data,
    /// letting players start playback immediately. Icecast-compatible
    /// (default 65536). Set to 0 to disable.
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
    /// ICY in-band metadata interval: number of audio bytes between metadata
    /// blocks injected into MP3 listener streams (advertised as the
    /// `icy-metaint` response header to clients that send `Icy-MetaData: 1`).
    /// Icecast-compatible (default 16000). Applies to MP3 output only —
    /// Vorbis streams carry metadata in native Vorbis comment headers and
    /// never advertise `icy-metaint`.
    #[serde(default = "default_icy_metaint")]
    pub icy_metaint: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountConfig {
    pub path: String,
    pub source_password: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_listeners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genre: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcode: Option<TranscodeConfig>,
    /// Per-mount override of [`LimitsConfig::burst_size`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TranscodeFormat {
    Mp3,
    Vorbis,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TranscodeConfig {
    pub format: TranscodeFormat,
    pub sample_rate: u32,
    pub bitrate_kbps: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Shuffle,
    Sequential,
}

fn default_order() -> Order {
    Order::Shuffle
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AutoDjConfig {
    pub mount: String,
    pub folder: std::path::PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genre: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "loop", default = "default_true")]
    pub loop_playlist: bool,
    #[serde(default = "default_order")]
    pub order: Order,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_listeners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst_size: Option<u32>,
    pub transcode: TranscodeConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RelayConfig {
    pub mount: String,
    pub upstream: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genre: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_listeners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcode: Option<TranscodeConfig>,
}

/// Reserved for v2 ACME / Let's Encrypt support. Parsed but unused in v1.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TlsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acme_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_cache_dir: Option<std::path::PathBuf>,
}

/// Load and parse the config file at the given path.
///
/// # Errors
/// Returns [`crate::error::ConfigError`] if the file cannot be read or parsed.
pub fn load(path: &std::path::Path) -> Result<Config, crate::error::ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    let cfg: Config = parse_str(&contents)?;
    cfg.validate_paths()
        .map_err(crate::error::ConfigError::Invalid)?;
    Ok(cfg)
}

/// Parse a TOML config from a string.
///
/// # Errors
/// Returns [`crate::error::ConfigError`] if the TOML cannot be parsed.
pub fn parse_str(toml_text: &str) -> Result<Config, crate::error::ConfigError> {
    let config = toml::from_str(toml_text)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_CONFIG: &str = r#"
[server]
stream_bind = "0.0.0.0:8000"
admin_bind  = "127.0.0.1:8001"
hostname    = "localhost"

[logging]
level  = "info"
format = "json"

[auth]
[[auth.users]]
username        = "admin"
password_bcrypt = "$2b$12$placeholder000000000000000000000"

[limits]
max_listeners_global  = 500
ring_size             = 64
slow_listener_grace_s = 2

[transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128

[[mounts]]
path            = "/stream"
source_password = "hackme"
max_listeners   = 100
name            = "My Radio"
description     = "The best radio"
genre           = "Electronic"
url             = "https://example.com"

[mounts.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 192
"#;

    const MINIMAL_CONFIG: &str = r#"
[server]
stream_bind = "0.0.0.0:8000"
admin_bind  = "127.0.0.1:8001"
hostname    = "localhost"

[logging]
level  = "info"
format = "pretty"

[auth]

[limits]
max_listeners_global  = 100
ring_size             = 32
slow_listener_grace_s = 5
"#;

    #[test]
    fn parses_full_config() {
        let cfg: Config = toml::from_str(FULL_CONFIG).unwrap();
        assert_eq!(cfg.server.hostname, "localhost");
        assert_eq!(cfg.auth.users.len(), 1);
        assert_eq!(cfg.auth.users[0].username, "admin");
        assert_eq!(cfg.mounts.len(), 1);
        assert_eq!(cfg.mounts[0].path, "/stream");
        assert_eq!(cfg.mounts[0].max_listeners, Some(100));
        assert_eq!(cfg.mounts[0].name.as_deref(), Some("My Radio"));
    }

    #[test]
    fn parses_minimal_config_no_mounts() {
        let cfg: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        assert!(cfg.mounts.is_empty());
        assert!(cfg.auth.users.is_empty());
        assert_eq!(cfg.limits.ring_size, 32);
    }

    #[test]
    fn mount_optional_fields_default_to_none() {
        let src = r#"
[server]
stream_bind = "0.0.0.0:8000"
admin_bind  = "127.0.0.1:8001"
hostname    = "localhost"
[logging]
level = "info"
format = "json"
[auth]
[limits]
max_listeners_global  = 500
ring_size             = 64
slow_listener_grace_s = 2
[[mounts]]
path            = "/radio"
source_password = "secret"
"#;
        let cfg: Config = toml::from_str(src).unwrap();
        assert!(cfg.mounts[0].name.is_none());
        assert!(cfg.mounts[0].description.is_none());
        assert!(cfg.mounts[0].max_listeners.is_none());
    }

    #[test]
    fn log_format_json_and_pretty() {
        let cfg: Config = toml::from_str(FULL_CONFIG).unwrap();
        assert_eq!(cfg.logging.format, LogFormat::Json);
        let cfg2: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(cfg2.logging.format, LogFormat::Pretty);
    }

    #[test]
    fn stream_bind_parses_as_socket_addr() {
        let cfg: Config = toml::from_str(FULL_CONFIG).unwrap();
        assert_eq!(cfg.server.stream_bind.port(), 8000);
        assert_eq!(cfg.server.admin_bind.port(), 8001);
        assert!(cfg.server.admin_bind.ip().is_loopback());
    }

    #[test]
    fn config_round_trips_through_toml() {
        let cfg: Config = toml::from_str(FULL_CONFIG).unwrap();
        let serialized = toml::to_string(&cfg).unwrap();
        let cfg2: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(cfg.server.hostname, cfg2.server.hostname);
        assert_eq!(cfg.mounts[0].path, cfg2.mounts[0].path);
    }

    const BASE_CONFIG: &str = r#"
[server]
stream_bind = "0.0.0.0:8000"
admin_bind  = "127.0.0.1:8001"
hostname    = "localhost"

[logging]
level  = "info"
format = "json"

[auth]

[limits]
max_listeners_global  = 500
ring_size             = 64
slow_listener_grace_s = 2
"#;

    #[test]
    fn transcode_config_parses_global() {
        let src = format!(
            r#"{BASE_CONFIG}
[transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let tc = cfg.transcode.as_ref().unwrap();
        assert_eq!(tc.format, TranscodeFormat::Mp3);
        assert_eq!(tc.sample_rate, 44100);
        assert_eq!(tc.bitrate_kbps, 128);
    }

    #[test]
    fn transcode_config_per_mount_overrides_global() {
        let src = format!(
            r#"{BASE_CONFIG}
[transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128

[[mounts]]
path            = "/stream"
source_password = "secret"

[mounts.transcode]
format       = "mp3"
sample_rate  = 22050
bitrate_kbps = 64
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let effective = cfg.effective_transcode(&cfg.mounts[0]).unwrap();
        assert_eq!(effective.sample_rate, 22050);
        assert_eq!(effective.bitrate_kbps, 64);
    }

    #[test]
    fn transcode_config_falls_back_to_global() {
        let src = format!(
            r#"{BASE_CONFIG}
[transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128

[[mounts]]
path            = "/stream"
source_password = "secret"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let effective = cfg.effective_transcode(&cfg.mounts[0]).unwrap();
        assert_eq!(effective.sample_rate, 44100);
        assert_eq!(effective.bitrate_kbps, 128);
    }

    #[test]
    fn transcode_format_parses_vorbis() {
        let src = format!(
            r#"{BASE_CONFIG}
[transcode]
format       = "vorbis"
sample_rate  = 44100
bitrate_kbps = 96
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let tc = cfg.transcode.as_ref().unwrap();
        assert_eq!(tc.format, TranscodeFormat::Vorbis);
        assert_eq!(tc.bitrate_kbps, 96);
    }

    #[test]
    fn burst_size_defaults_to_65536_when_absent() {
        let cfg: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(cfg.limits.burst_size, 65_536);
    }

    #[test]
    fn icy_metaint_defaults_to_16000_when_absent() {
        let cfg: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(cfg.limits.icy_metaint, 16_000);
    }

    #[test]
    fn icy_metaint_is_configurable() {
        let src = format!(
            r#"{BASE_CONFIG}
[limits]
max_listeners_global  = 500
ring_size             = 64
slow_listener_grace_s = 2
burst_size            = 65536
icy_metaint           = 8192
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert_eq!(cfg.limits.icy_metaint, 8_192);
    }

    #[test]
    fn burst_size_per_mount_overrides_global() {
        let src = format!(
            r#"{BASE_CONFIG}
[[mounts]]
path            = "/stream"
source_password = "secret"
burst_size      = 131072
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        // Global default kicks in from serde default:
        assert_eq!(cfg.limits.burst_size, 65_536);
        // Per-mount override wins:
        assert_eq!(cfg.effective_burst_size(&cfg.mounts[0]), 131_072);
    }

    #[test]
    fn burst_size_falls_back_to_global() {
        let src = format!(
            r#"{BASE_CONFIG}
[[mounts]]
path            = "/stream"
source_password = "secret"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert!(cfg.mounts[0].burst_size.is_none());
        assert_eq!(cfg.effective_burst_size(&cfg.mounts[0]), 65_536);
    }

    #[test]
    fn transcode_config_none_when_absent() {
        let src = format!(
            r#"{BASE_CONFIG}
[[mounts]]
path            = "/stream"
source_password = "secret"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert!(cfg.effective_transcode(&cfg.mounts[0]).is_none());
    }

    #[test]
    fn parses_autodj_entry() {
        let src = format!(
            r#"{BASE_CONFIG}
[[autodjs]]
mount        = "/lofi"
name         = "Lo-Fi"
description  = "study channel"
genre        = "Lo-Fi"
folder       = "/var/lib/rustyice/lofi"
order        = "shuffle"

[autodjs.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert_eq!(cfg.autodjs.len(), 1);
        let a = &cfg.autodjs[0];
        assert_eq!(a.mount, "/lofi");
        assert_eq!(a.name.as_deref(), Some("Lo-Fi"));
        assert_eq!(a.folder, std::path::PathBuf::from("/var/lib/rustyice/lofi"));
        assert!(a.enabled);
        assert!(a.loop_playlist);
        assert!(matches!(a.order, Order::Shuffle));
        assert_eq!(a.transcode.bitrate_kbps, 128);
    }

    #[test]
    fn autodj_order_defaults_to_shuffle_when_omitted() {
        let src = format!(
            r#"{BASE_CONFIG}
[[autodjs]]
mount  = "/x"
folder = "/tmp"

[autodjs.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert!(matches!(cfg.autodjs[0].order, Order::Shuffle));
        assert!(cfg.autodjs[0].enabled);
        assert!(cfg.autodjs[0].loop_playlist);
    }

    #[test]
    fn autodj_mount_path_collision_with_mounts_is_detected() {
        let src = format!(
            r#"{BASE_CONFIG}
[[mounts]]
path            = "/dup"
source_password = "x"

[[autodjs]]
mount  = "/dup"
folder = "/tmp"

[autodjs.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let err = cfg.validate_paths().unwrap_err();
        assert!(err.contains("/dup"));
    }

    #[test]
    fn duplicate_autodj_paths_are_detected() {
        let src = format!(
            r#"{BASE_CONFIG}
[[autodjs]]
mount  = "/dup"
folder = "/tmp"
[autodjs.transcode]
format = "mp3"
sample_rate = 44100
bitrate_kbps = 128

[[autodjs]]
mount  = "/dup"
folder = "/tmp"
[autodjs.transcode]
format = "mp3"
sample_rate = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let err = cfg.validate_paths().unwrap_err();
        assert!(err.contains("/dup"));
    }

    #[test]
    fn unique_autodj_and_mount_paths_validate_ok() {
        let src = format!(
            r#"{BASE_CONFIG}
[[mounts]]
path            = "/live"
source_password = "x"

[[autodjs]]
mount  = "/auto"
folder = "/tmp"

[autodjs.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        cfg.validate_paths().unwrap();
    }

    #[test]
    fn parses_relay_entry() {
        let src = format!(
            r#"{BASE_CONFIG}
[[relays]]
mount        = "/relay"
upstream     = "http://upstream.example.com:8000/jazz"
name         = "Jazz Relay"
username     = "relay"
password     = "secret"

[relays.transcode]
format       = "mp3"
sample_rate  = 44100
bitrate_kbps = 128
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert_eq!(cfg.relays.len(), 1);
        let r = &cfg.relays[0];
        assert_eq!(r.mount, "/relay");
        assert_eq!(r.upstream, "http://upstream.example.com:8000/jazz");
        assert_eq!(r.name.as_deref(), Some("Jazz Relay"));
        assert_eq!(r.username.as_deref(), Some("relay"));
        assert_eq!(r.password.as_deref(), Some("secret"));
        assert!(r.enabled);
        assert_eq!(r.transcode.as_ref().unwrap().bitrate_kbps, 128);
    }

    #[test]
    fn relay_with_no_transcode_parses_ok() {
        let src = format!(
            r#"{BASE_CONFIG}
[[relays]]
mount    = "/relay"
upstream = "http://upstream.example.com/stream"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        assert!(cfg.relays[0].transcode.is_none());
        assert!(cfg.relays[0].enabled);
        assert!(cfg.relays[0].username.is_none());
        assert!(cfg.relays[0].password.is_none());
    }

    #[test]
    fn relay_mount_path_collision_with_autodj_is_detected() {
        let src = format!(
            r#"{BASE_CONFIG}
[[autodjs]]
mount  = "/dup"
folder = "/tmp"
[autodjs.transcode]
format = "mp3"
sample_rate = 44100
bitrate_kbps = 128

[[relays]]
mount    = "/dup"
upstream = "http://example.com/x"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let err = cfg.validate_paths().unwrap_err();
        assert!(err.contains("/dup"));
    }

    #[test]
    fn relay_partial_credentials_are_rejected() {
        let src = format!(
            r#"{BASE_CONFIG}
[[relays]]
mount    = "/r"
upstream = "http://example.com/x"
username = "user"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let err = cfg.validate_paths().unwrap_err();
        assert!(err.to_lowercase().contains("username"));
    }

    #[test]
    fn relay_with_non_http_upstream_is_rejected() {
        let src = format!(
            r#"{BASE_CONFIG}
[[relays]]
mount    = "/r"
upstream = "ftp://example.com/x"
"#
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let err = cfg.validate_paths().unwrap_err();
        assert!(err.to_lowercase().contains("upstream"));
    }
}
