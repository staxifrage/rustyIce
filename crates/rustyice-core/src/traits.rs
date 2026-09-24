use crate::error::{AuthError, CodecError, IngestError, OutputError};
use crate::types::{
    CodecId, CodecInfo, EncodeConfig, EncodedPacket, ListenerStats, PcmFrame, SourceStats,
    StreamPacket,
};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

// ── Codec ──────────────────────────────────────────────────────────────────

/// Identifies, decodes, and encodes a single audio format.
///
/// v1 MP3 impl is probe-only: `decode` and `encode` return
/// `CodecError::Unsupported` until the transcoding pipeline is added.
pub trait Codec: Send + Sync + 'static {
    fn codec_id(&self) -> CodecId;

    /// Inspect the first N bytes of a stream.
    /// Returns `Some(CodecInfo)` if this codec recognises the data, `None` otherwise.
    fn probe(&self, header: &[u8]) -> Option<CodecInfo>;

    /// Decode one encoded packet to PCM.
    ///
    /// # Errors
    /// Returns [`CodecError`] if the packet is malformed or unsupported.
    fn decode(&self, packet: &EncodedPacket) -> Result<PcmFrame, CodecError>;

    /// Encode a PCM frame to this codec's format.
    ///
    /// # Errors
    /// Returns [`CodecError`] if encoding fails.
    fn encode(&self, frame: &PcmFrame, config: &EncodeConfig) -> Result<EncodedPacket, CodecError>;
}

// ── Fan-out bus ────────────────────────────────────────────────────────────

/// Fan-out bus for a single mount point.
///
/// v1: `TokioBroadcastBus` (wraps `tokio::sync::broadcast`).
/// v2: swap for a custom lock-free slot ring — no consumers change.
pub trait BroadcastBus: Send + Sync + 'static {
    /// Publish a packet to all current subscribers. Non-blocking.
    /// Packets are `Arc`-wrapped so subscribers pay only a pointer clone.
    fn publish(&self, packet: Arc<StreamPacket>);

    /// Create a new subscriber. Receives packets from this point forward,
    /// preceded by a burst-on-connect prefix of recent stream bytes bounded
    /// by [`LimitsConfig::burst_size`](crate::config::LimitsConfig::burst_size)
    /// (or its per-mount override). Lets clients fill playback buffers
    /// instantly instead of waiting for live data at real-time rates.
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = Arc<StreamPacket>> + Send + 'static>>;

    /// Like [`subscribe`](Self::subscribe), but does **not** prepend the
    /// burst prefix. Used by Vorbis listeners that join mid-stream: the
    /// stream router has already written the captured Vorbis header pages to
    /// the response, and history would replay them a second time — which
    /// libvorbis treats as an unexpected stream restart, producing a brief
    /// blip followed by silence.
    fn subscribe_live(&self) -> Pin<Box<dyn Stream<Item = Arc<StreamPacket>> + Send + 'static>> {
        // Default falls back to the history-bearing variant so existing
        // implementations don't have to opt in.
        self.subscribe()
    }

    /// Instantaneous subscriber count (for admin API and /metrics).
    fn subscriber_count(&self) -> usize;
}

// ── Ingest protocol ────────────────────────────────────────────────────────

/// Handles the audio byte stream from a source after HTTP auth completes.
///
/// `run` takes `AsyncRead` so RTMP / SRT / WebRTC can wrap their own
/// transports without depending on axum.
#[async_trait]
pub trait IngestProtocol: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Read packets from the source and publish them to `bus` until the
    /// source disconnects or `cancellation` is triggered.
    async fn run(
        &self,
        reader: Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
        bus: Arc<dyn BroadcastBus>,
        codec: CodecId,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<SourceStats, IngestError>;
}

// ── Output protocol ────────────────────────────────────────────────────────

/// Handles writing stream data to a listener after HTTP negotiation.
#[async_trait]
pub trait OutputProtocol: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Stream packets to the listener until it disconnects, is kicked,
    /// or `cancellation` is triggered.
    ///
    /// When `has_icy_metadata` is true (MP3 listener that sent
    /// `Icy-MetaData: 1` and was answered with `icy-metaint`), exactly
    /// `metaint` audio bytes are written between in-band metadata blocks;
    /// the metadata frames themselves do not count toward the interval.
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        writer: Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
        subscription: Pin<Box<dyn Stream<Item = Arc<StreamPacket>> + Send>>,
        mount_info: Arc<crate::mount::MountInfo>,
        current_title: Arc<arc_swap::ArcSwap<Option<String>>>,
        source_overlay: Arc<arc_swap::ArcSwap<Option<crate::mount::SourceOverlay>>>,
        has_icy_metadata: bool,
        last_meta_payload: &mut Option<Vec<u8>>,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<ListenerStats, OutputError>;
}

// ── Auth backend ───────────────────────────────────────────────────────────

/// Verifies credentials for admin access and per-mount source access.
///
/// v1: bcrypt + TOML user table.
/// v2: OIDC, `WebAuthn`, bearer tokens — all satisfy this interface.
#[async_trait]
pub trait AuthBackend: Send + Sync + 'static {
    async fn verify_admin(&self, username: &str, password: &str) -> Result<bool, AuthError>;
    async fn verify_source(&self, mount_path: &str, password: &str) -> Result<bool, AuthError>;

    /// Verifies the global default source password. Used to authenticate
    /// sources that want to create a new mount not present in `[[mounts]]`.
    /// Default implementation rejects all attempts (no default password set).
    async fn verify_default_source(&self, _password: &str) -> Result<bool, AuthError> {
        Ok(false)
    }

    /// Called on SIGHUP. Re-read credentials from the backing store.
    async fn reload(&self, config: &crate::config::Config) -> Result<(), AuthError>;
}

// ── Object-safety tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::types::{CodecId, CodecInfo, EncodeConfig, EncodedPacket, PcmFrame, StreamPacket};
    use futures::stream;

    struct StubCodec;
    impl Codec for StubCodec {
        fn codec_id(&self) -> CodecId { CodecId::MP3 }
        fn probe(&self, _: &[u8]) -> Option<CodecInfo> { None }
        fn decode(&self, _: &EncodedPacket) -> Result<PcmFrame, CodecError> {
            Err(CodecError::Unsupported)
        }
        fn encode(&self, _: &PcmFrame, _: &EncodeConfig) -> Result<EncodedPacket, CodecError> {
            Err(CodecError::Unsupported)
        }
    }

    struct StubBus;
    impl BroadcastBus for StubBus {
        fn publish(&self, _: Arc<StreamPacket>) {}
        fn subscribe(&self) -> Pin<Box<dyn Stream<Item = Arc<StreamPacket>> + Send + 'static>> {
            Box::pin(stream::empty())
        }
        fn subscriber_count(&self) -> usize { 0 }
    }

    #[test]
    fn codec_is_object_safe() {
        let _: &dyn Codec = &StubCodec;
        let _: Box<dyn Codec> = Box::new(StubCodec);
    }

    #[test]
    fn broadcast_bus_is_object_safe() {
        let _: Arc<dyn BroadcastBus> = Arc::new(StubBus);
    }

    #[test]
    fn stub_bus_subscriber_count() {
        let bus = StubBus;
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[test]
    fn stub_bus_subscribe_returns_empty_stream() {
        let bus = StubBus;
        let _sub = bus.subscribe();
    }
}
