use crate::state::AppState;
use crate::stream_listener::PeerAddr;
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rustyice_core::config::TranscodeFormat;
use rustyice_core::mount::MountStats;
use rustyice_core::types::CodecId;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Builds the router for listener traffic on the stream port.
///
/// Source uploads (PUT/SOURCE) are intercepted by
/// `crate::source_protocol::handle_source_connection` *before* the connection
/// reaches axum, because the Icecast source protocol uses request framing that
/// hyper rejects as non-RFC-compliant HTTP/1.1 (no `Content-Length`, no
/// `Transfer-Encoding`, `Expect: 100-continue` without auto-response).
pub fn build_stream_router(state: AppState) -> Router {
    Router::new()
        .route("/{mount}", get(listener_handler))
        .with_state(state)
}

/// Returns the transcode config in effect for `mount_path`, falling back to
/// the global `[transcode]` block for dynamic mounts.
pub(crate) fn mount_transcode(
    cfg: &rustyice_core::config::Config,
    mount_path: &str,
) -> Option<rustyice_core::config::TranscodeConfig> {
    if let Some(mc) = cfg.mounts.iter().find(|m| m.path == mount_path) {
        cfg.effective_transcode(mc).cloned()
    } else {
        cfg.transcode.clone()
    }
}

async fn listener_handler(
    State(state): State<AppState>,
    Path(mount_segment): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerAddr>,
    headers: HeaderMap,
) -> Response {
    let mount_path = format!("/{mount_segment}");
    let peer_addr = Some(peer.0);

    let Some(mount) = state.mounts.get(&mount_path) else {
        return (StatusCode::NOT_FOUND, "mount not found").into_response();
    };

    let cfg = state.config.load();
    let global_count = state.listeners.total_count();
    if global_count >= cfg.limits.max_listeners_global as usize {
        return (StatusCode::SERVICE_UNAVAILABLE, "server full").into_response();
    }
    if let Some(max) = mount.info.load().max_listeners
        && mount.listener_count() >= max as usize
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "mount full").into_response();
    }

    let mount_info = mount.info.load_full();
    let transcode = mount_transcode(&cfg, &mount_path);
    let output_codec = resolve_output_codec(&mount_info.codec, transcode.as_ref());
    let content_type = content_type_for(&output_codec);
    // ICY interleaved metadata is an MP3-only convention.  Vorbis streams
    // carry metadata in-stream as Vorbis comments, and injecting an ICY
    // byte block into the Ogg stream would corrupt page framing — so we
    // never honour (or advertise) it for Vorbis output.
    let has_icy_metadata = output_codec == CodecId::MP3
        && headers
            .get("icy-metadata")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.trim() == "1");
    // Bytes of audio between in-band metadata blocks; advertised to the
    // listener as `icy-metaint` and used by the output writer's per-
    // connection injection counter. Configurable via [limits].icy_metaint.
    let icy_metaint = cfg.limits.icy_metaint.max(1);
    let header_bytes_snap = mount.header_bytes.load_full();

    let current_title = mount.current_title.clone();
    let source_overlay = mount.source_overlay.clone();
    // When we've captured Vorbis header pages and are about to prepend them
    // to the response, skip the bus's history snapshot — otherwise the
    // listener would receive the same headers twice (once via prepend, once
    // via history) and libvorbis would treat the second set as a stream
    // restart, producing a brief blip followed by silence.
    let subscription = if header_bytes_snap.as_ref().is_some() {
        mount.bus.subscribe_live()
    } else {
        mount.bus.subscribe()
    };

    let listener_cancel = state.shutdown.child_token();
    let listener_id = state
        .listeners
        .register(mount_path.clone(), peer_addr, listener_cancel.clone());

    let (read_end, write_end) = tokio::io::duplex(65_536);
    // Wrap the duplex writer so every byte the output protocol writes is
    // reflected in `mount.stats.bytes_sent` for the live outbound-bandwidth
    // gauge. Counted bytes include captured Vorbis header pages, ICY
    // metadata frames, and the audio payload — i.e. anything the listener
    // sees on the wire.
    let writer: Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> = Box::pin(
        CountingWriter::new(write_end, mount.stats.clone()),
    );

    let output = state.output.clone();
    let listeners_ref = state.listeners.clone();
    let cancel_clone = listener_cancel.clone();

    tokio::spawn(async move {
        let mut writer = writer;
        // Tracks the last ICY metadata payload sent to this listener so the
        // output protocol can suppress duplicate in-band metadata frames.
        let mut last_meta_payload: Option<Vec<u8>> = None;
        // Vorbis listeners joining mid-broadcast need the ident/comment/setup
        // header pages prepended before the live bus stream — without them
        // libvorbis on the client cannot decode any audio packets.  For MP3
        // `header_bytes_snap` stays `None` and this is a no-op.
        if let Some(headers) = header_bytes_snap.as_ref().as_ref() {
            if let Err(e) = writer.write_all(headers).await {
                tracing::warn!("listener {listener_id}: header prefix write failed: {e}");
                listeners_ref.deregister(listener_id);
                return;
            }
        }
        match output
            .run(writer, subscription, mount_info, current_title, source_overlay, has_icy_metadata, &mut last_meta_payload, cancel_clone)
            .await
        {
            Ok(listener_stats) => {
                tracing::info!(
                    "listener output ended: id={listener_id} bytes={} duration={:?} reason={:?}",
                    listener_stats.bytes_sent, listener_stats.duration, listener_stats.disconnect_reason,
                );
            }
            Err(rustyice_core::error::OutputError::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::UnexpectedEof,
                ) =>
            {
                // Listener (browser/player) closed the connection. Expected,
                // not a server problem — keep it out of the warn stream.
                tracing::debug!("listener disconnected: id={listener_id} kind={:?}", e.kind());
            }
            Err(rustyice_core::error::OutputError::Cancelled) => {
                tracing::debug!("listener output cancelled: id={listener_id}");
            }
            Err(e) => {
                tracing::warn!("listener output errored: id={listener_id} err={e}");
            }
        }
        listeners_ref.deregister(listener_id);
    });

    let stream = tokio_util::io::ReaderStream::new(read_end);

    let identity = mount.effective_identity(transcode.as_ref());

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        // Allow the browser Web Audio API to analyse the stream cross-origin:
        // the player UI is served from the admin port, not the stream port.
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");

    if let Some(v) = &identity.name {
        builder = builder.header("icy-name", v);
    }
    if let Some(v) = &identity.description {
        builder = builder.header("icy-description", v);
    }
    if let Some(v) = &identity.genre {
        builder = builder.header("icy-genre", v);
    }
    if let Some(v) = &identity.url {
        builder = builder.header("icy-url", v);
    }
    if let Some(p) = identity.public {
        builder = builder.header("icy-pub", if p { "1" } else { "0" });
    }
    if let Some(v) = &identity.audio_info {
        builder = builder.header("ice-audio-info", v);
    }
    if let Some(b) = identity.bitrate_kbps {
        builder = builder.header("icy-br", b.to_string());
    }

    if has_icy_metadata {
        builder = builder.header("icy-metaint", icy_metaint.to_string());
    }

    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// The codec actually written to the listener: the transcode target if one
/// is configured, otherwise the passthrough source codec.
fn resolve_output_codec(
    source_codec: &CodecId,
    transcode: Option<&rustyice_core::config::TranscodeConfig>,
) -> CodecId {
    match transcode.map(|tc| &tc.format) {
        Some(TranscodeFormat::Mp3) => CodecId::MP3,
        Some(TranscodeFormat::Vorbis) => CodecId::VORBIS,
        None => source_codec.clone(),
    }
}

fn content_type_for(codec: &CodecId) -> &'static str {
    match *codec {
        CodecId::VORBIS => "application/ogg",
        _ => "audio/mpeg",
    }
}

/// Writer that ticks `mount.stats.bytes_sent` for every byte successfully
/// written to its inner writer. Wraps the listener-side duplex `write_end`
/// so the count reflects what the output protocol delivered toward the HTTP
/// response body (and thus the listener).
struct CountingWriter<W> {
    inner: W,
    stats: Arc<MountStats>,
}

impl<W: AsyncWrite + Unpin> CountingWriter<W> {
    fn new(inner: W, stats: Arc<MountStats>) -> Self {
        Self { inner, stats }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &result {
            self.stats
                .bytes_sent
                .fetch_add(*n as u64, Ordering::Relaxed);
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
