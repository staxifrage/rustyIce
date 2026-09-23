use async_trait::async_trait;
use futures::StreamExt;
use rustyice_core::error::OutputError;
use rustyice_core::mount::{MountInfo, SourceOverlay};
use rustyice_core::traits::OutputProtocol;
use rustyice_core::types::{AudioPayload, DisconnectReason, ListenerStats, StreamPacket};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub struct HttpPassthroughOutput {
    /// Bytes between ICY metadata frames. Standard value: 16000 (advertised
    /// to clients as `icy-metaint`; configurable via `[limits].icy_metaint`).
    pub icy_metaint: usize,
}

impl Default for HttpPassthroughOutput {
    fn default() -> Self {
        Self { icy_metaint: 16_000 }
    }
}

impl HttpPassthroughOutput {
    /// Build an output with the metaint from `[limits].icy_metaint`. A value
    /// of 0 falls back to the Icecast-compatible default (16000) so the
    /// in-band framing can never divide by zero.
    #[must_use]
    pub fn with_metaint(metaint: u32) -> Self {
        Self { icy_metaint: if metaint == 0 { 16_000 } else { metaint as usize } }
    }
}

#[async_trait]
impl OutputProtocol for HttpPassthroughOutput {
    fn name(&self) -> &'static str {
        "http-passthrough"
    }

    async fn run(
        &self,
        mut writer: Pin<Box<dyn AsyncWrite + Send + Unpin>>,
        mut subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>>,
        mount_info: Arc<MountInfo>,
        current_title: Arc<arc_swap::ArcSwap<Option<String>>>,
        source_overlay: Arc<arc_swap::ArcSwap<Option<SourceOverlay>>>,
        has_icy_metadata: bool,
        last_meta_payload: &mut Option<Vec<u8>>,
        cancellation: CancellationToken,
    ) -> Result<ListenerStats, OutputError> {
        let start = Instant::now();
        let mut bytes_sent: u64 = 0;
        // Connection-specific ICY state: audio bytes written since the last
        // in-band metadata block, and the metadata payload last injected on
        // *this* connection. Listeners join at different moments, so the
        // counter must live per writer loop — never on the shared bus.
        // Per spec, `bytes_sent_since_meta` counts only audio bytes; the
        // metadata frames themselves are tracked in `bytes_sent` (wire total).
        let mut bytes_sent_since_meta: usize = 0;
        let mut disconnect_reason = DisconnectReason::SourceEnded;

        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    disconnect_reason = DisconnectReason::ShuttingDown;
                    break;
                }
                item = subscription.next() => {
                    let Some(packet) = item else { break; };
                    let AudioPayload::Encoded(ref enc) = packet.payload else {
                        continue;
                    };
                    let data = &enc.data;

                    if has_icy_metadata {
                        write_with_icy(
                            &mut writer,
                            data,
                            &mount_info,
                            &current_title,
                            &source_overlay,
                            self.icy_metaint,
                            &mut bytes_sent_since_meta,
                            last_meta_payload,
                            &mut bytes_sent,
                        )
                        .await?;
                    } else {
                        writer.write_all(data).await.map_err(OutputError::Io)?;
                        bytes_sent += data.len() as u64;
                    }
                }
            }
        }

        debug!("listener disconnecting: reason={disconnect_reason:?} bytes={bytes_sent}");
        Ok(ListenerStats {
            bytes_sent,
            duration: start.elapsed(),
            disconnect_reason,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_with_icy(
    writer: &mut Pin<Box<dyn AsyncWrite + Send + Unpin>>,
    data: &bytes::Bytes,
    info: &MountInfo,
    current_title: &Arc<arc_swap::ArcSwap<Option<String>>>,
    source_overlay: &Arc<arc_swap::ArcSwap<Option<SourceOverlay>>>,
    metaint: usize,
    bytes_sent_since_meta: &mut usize,
    last_meta_payload: &mut Option<Vec<u8>>,
    bytes_sent: &mut u64,
) -> Result<(), OutputError> {
    let mut offset = 0;
    while offset < data.len() {
        // Write audio only up to the next icy-metaint boundary; if this chunk
        // crosses it, split the write and pause the audio for the meta block.
        let space = metaint - *bytes_sent_since_meta;
        let chunk_end = (offset + space).min(data.len());
        let chunk = &data[offset..chunk_end];
        writer.write_all(chunk).await.map_err(OutputError::Io)?;
        *bytes_sent += chunk.len() as u64;
        *bytes_sent_since_meta += chunk.len();
        offset = chunk_end;

        if *bytes_sent_since_meta >= metaint {
            let frame = build_icy_frame(info, current_title, source_overlay, last_meta_payload);
            writer.write_all(&frame).await.map_err(OutputError::Io)?;
            // Meta frames don't count toward the audio interval — the
            // listener's parser resyncs on the length byte and expects
            // exactly `metaint` audio bytes before the next block.
            *bytes_sent_since_meta = 0;
        }
    }
    Ok(())
}

/// Resolve the effective now-playing title for this mount: the live/AutoDJ/
/// admin-set title wins, then the source-supplied name overlay, then the
/// configured mount name.
fn effective_title(
    info: &MountInfo,
    current_title: &Arc<arc_swap::ArcSwap<Option<String>>>,
    source_overlay: &Arc<arc_swap::ArcSwap<Option<SourceOverlay>>>,
) -> String {
    let title_snap = current_title.load_full();
    let overlay_snap = source_overlay.load_full();
    let overlay_ref = overlay_snap.as_ref().as_ref();
    title_snap
        .as_deref()
        .or_else(|| overlay_ref.and_then(|o| o.name.as_deref()))
        .or(info.metadata.name.as_deref())
        .unwrap_or("")
        .to_string()
}

fn build_icy_frame(
    info: &MountInfo,
    current_title: &Arc<arc_swap::ArcSwap<Option<String>>>,
    source_overlay: &Arc<arc_swap::ArcSwap<Option<SourceOverlay>>>,
    last_meta_payload: &mut Option<Vec<u8>>,
) -> Vec<u8> {
    let overlay_snap = source_overlay.load_full();
    let overlay_ref = overlay_snap.as_ref().as_ref();

    let title = effective_title(info, current_title, source_overlay);
    let url = overlay_ref
        .and_then(|o| o.url.as_deref())
        .or(info.metadata.url.as_deref())
        .unwrap_or("");
    let meta_str = format!("StreamTitle='{title}';StreamUrl='{url}';");
    let payload = meta_str.into_bytes();

    // Per-connection change detection: if the payload is identical to what
    // was last injected on this connection, send a zero-length block so the
    // player keeps its current "Now Playing" display without re-parsing.
    if last_meta_payload.as_deref() == Some(payload.as_slice()) {
        return vec![0x00];
    }
    if payload.is_empty() {
        return vec![0x00];
    }
    *last_meta_payload = Some(payload.clone());

    let raw = payload.as_slice();
    let blocks = raw.len().div_ceil(16);
    let padded = blocks * 16;
    let mut frame = Vec::with_capacity(1 + padded);
    frame.push(u8::try_from(blocks).unwrap_or(u8::MAX));
    frame.extend_from_slice(raw);
    frame.resize(1 + padded, 0);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use rustyice_core::mount::{MountInfo, MountMetadata, SourceOverlay};
    use rustyice_core::types::{AudioPayload, CodecId, EncodedPacket, StreamPacket};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    fn make_packet(data: &[u8]) -> Arc<StreamPacket> {
        Arc::new(StreamPacket {
            payload: AudioPayload::Encoded(EncodedPacket {
                codec: CodecId::MP3,
                data: Bytes::copy_from_slice(data),
            }),
            pts: Duration::ZERO,
            sequence: 0,
        })
    }

    fn empty_title() -> Arc<arc_swap::ArcSwap<Option<String>>> {
        Arc::new(arc_swap::ArcSwap::from_pointee(None))
    }

    fn empty_overlay() -> Arc<arc_swap::ArcSwap<Option<SourceOverlay>>> {
        Arc::new(arc_swap::ArcSwap::from_pointee(None))
    }

    /// Per-connection "last injected payload" tracker (starts empty).
    fn no_last_meta() -> Option<Vec<u8>> {
        None
    }

    fn make_mount_info(name: Option<&str>) -> Arc<MountInfo> {
        Arc::new(MountInfo {
            path: "/stream".to_string(),
            codec: CodecId::MP3,
            source_password: "x".to_string(),
            max_listeners: None,
            metadata: MountMetadata {
                name: name.map(str::to_string),
                description: None,
                genre: None,
                url: None,
            },
        })
    }

    #[tokio::test]
    async fn passthrough_writes_bytes_verbatim() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let payload = b"hello audio world";
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(payload)]));

        let stats = HttpPassthroughOutput::default()
            .run(writer, subscription, make_mount_info(None), empty_title(), empty_overlay(), &mut no_last_meta(), false, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload);
        assert_eq!(stats.bytes_sent, payload.len() as u64);
    }

    #[tokio::test]
    async fn icy_injects_metadata_at_metaint_boundary() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let payload = vec![0xABu8; 16];
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(&payload)]));

        HttpPassthroughOutput { icy_metaint: 8 }
            .run(writer, subscription, make_mount_info(Some("Test")), empty_title(), empty_overlay(), &mut no_last_meta(), true, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        assert_eq!(&received[..8], &payload[..8]);
        assert!(received[8] >= 1);
        let meta_len = received[8] as usize * 16;
        let audio_start = 9 + meta_len;
        assert_eq!(&received[audio_start..audio_start + 8], &payload[8..]);
    }

    #[tokio::test]
    async fn icy_empty_metadata_writes_zero_byte() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(&[0u8; 4])]));

        HttpPassthroughOutput { icy_metaint: 4 }
            .run(writer, subscription, make_mount_info(None), empty_title(), empty_overlay(), &mut no_last_meta(), true, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        assert_eq!(received.len(), 5);
        assert_eq!(received[4], 0x00);
    }

    #[tokio::test]
    async fn cancellation_returns_shutting_down() {
        let (_read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::repeat(make_packet(&[0u8; 4])));
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let handle = tokio::spawn(async move {
            HttpPassthroughOutput::default()
                .run(writer, subscription, make_mount_info(None), empty_title(), empty_overlay(), &mut no_last_meta(), false, token_clone)
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        token.cancel();
        let stats = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("did not stop on cancellation")
            .unwrap()
            .unwrap();
        assert_eq!(stats.disconnect_reason, DisconnectReason::ShuttingDown);
    }

    fn parse_icy_meta(received: &[u8], metaint: usize) -> String {
        // After `metaint` audio bytes, the next byte is the meta length in 16-byte units.
        assert!(received.len() > metaint, "no metadata block in response");
        let len_byte = received[metaint] as usize;
        let meta_start = metaint + 1;
        let meta_end = meta_start + len_byte * 16;
        assert!(received.len() >= meta_end, "metadata truncated");
        let raw = &received[meta_start..meta_end];
        // Trim trailing NULs (padding to 16-byte block).
        let end = raw.iter().rposition(|&b| b != 0).map_or(0, |p| p + 1);
        String::from_utf8_lossy(&raw[..end]).into_owned()
    }

    #[tokio::test]
    async fn icy_uses_admin_title_when_set_over_mount_name() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let payload = vec![0xABu8; 16];
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(&payload)]));

        let title: Arc<arc_swap::ArcSwap<Option<String>>> =
            Arc::new(arc_swap::ArcSwap::from_pointee(Some("Artist - Song".to_string())));

        HttpPassthroughOutput { icy_metaint: 8 }
            .run(writer, subscription, make_mount_info(Some("Mount Name")), title, empty_overlay(), &mut no_last_meta(), true, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        let meta = parse_icy_meta(&received, 8);
        assert!(meta.contains("StreamTitle='Artist - Song';"),
            "admin title should win; got meta={meta:?}");
        assert!(!meta.contains("StreamTitle='Mount Name';"),
            "mount name must not be used when admin title is set");
    }

    #[tokio::test]
    async fn icy_falls_back_to_mount_name_when_title_unset() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let payload = vec![0xABu8; 16];
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(&payload)]));

        HttpPassthroughOutput { icy_metaint: 8 }
            .run(writer, subscription, make_mount_info(Some("Mount Name")), empty_title(), empty_overlay(), &mut no_last_meta(), true, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        let meta = parse_icy_meta(&received, 8);
        assert!(meta.contains("StreamTitle='Mount Name';"),
            "should fall back to mount name; got meta={meta:?}");
    }

    #[tokio::test]
    async fn icy_empty_when_both_title_and_name_unset() {
        let (mut read_end, write_end) = tokio::io::duplex(65_536);
        let writer: Pin<Box<dyn AsyncWrite + Send + Unpin>> = Box::pin(write_end);
        let subscription: Pin<Box<dyn futures::Stream<Item = Arc<StreamPacket>> + Send>> =
            Box::pin(stream::iter(vec![make_packet(&[0u8; 4])]));

        HttpPassthroughOutput { icy_metaint: 4 }
            .run(writer, subscription, make_mount_info(None), empty_title(), empty_overlay(), &mut no_last_meta(), true, CancellationToken::new())
            .await
            .unwrap();

        let mut received = Vec::new();
        read_end.read_to_end(&mut received).await.unwrap();
        // 4 audio bytes + 1 length byte (0 = no meta).
        assert_eq!(received.len(), 5);
        assert_eq!(received[4], 0x00);
    }

    #[tokio::test]
    async fn icy_meta_falls_back_to_overlay_name_when_no_admin_title() {
        use arc_swap::ArcSwap;
        // current_title = None, overlay.name = "From Source", config name = "Cfg"
        let info = Arc::new(MountInfo {
            path: "/m".to_string(),
            codec: CodecId::MP3,
            source_password: "x".to_string(),
            max_listeners: None,
            metadata: MountMetadata {
                name: Some("Cfg".to_string()),
                ..Default::default()
            },
        });
        let current_title = Arc::new(ArcSwap::from_pointee(None));
        let overlay = Arc::new(ArcSwap::from_pointee(Some(SourceOverlay {
            name: Some("From Source".to_string()),
            url: Some("https://overlay".to_string()),
            ..Default::default()
        })));
        let frame = build_icy_frame(&info, &current_title, &overlay, &mut no_last_meta());
        let s = String::from_utf8_lossy(&frame[1..]);
        assert!(
            s.contains("StreamTitle='From Source';"),
            "expected overlay name in StreamTitle, got: {s:?}"
        );
        assert!(
            s.contains("StreamUrl='https://overlay';"),
            "expected overlay url in StreamUrl, got: {s:?}"
        );
    }

    #[tokio::test]
    async fn icy_meta_admin_title_beats_overlay_name() {
        use arc_swap::ArcSwap;
        let info = Arc::new(MountInfo {
            path: "/m".to_string(),
            codec: CodecId::MP3,
            source_password: "x".to_string(),
            max_listeners: None,
            metadata: MountMetadata::default(),
        });
        let current_title = Arc::new(ArcSwap::from_pointee(Some("Artist - Song".to_string())));
        let overlay = Arc::new(ArcSwap::from_pointee(Some(SourceOverlay {
            name: Some("From Source".to_string()),
            ..Default::default()
        })));
        let frame = build_icy_frame(&info, &current_title, &overlay, &mut no_last_meta());
        let s = String::from_utf8_lossy(&frame[1..]);
        assert!(
            s.contains("StreamTitle='Artist - Song';"),
            "admin title must win, got: {s:?}"
        );
    }

    #[tokio::test]
    async fn icy_meta_falls_through_to_config_when_overlay_silent() {
        use arc_swap::ArcSwap;
        let info = Arc::new(MountInfo {
            path: "/m".to_string(),
            codec: CodecId::MP3,
            source_password: "x".to_string(),
            max_listeners: None,
            metadata: MountMetadata {
                name: Some("Cfg Name".to_string()),
                url: Some("https://cfg".to_string()),
                ..Default::default()
            },
        });
        let current_title = Arc::new(ArcSwap::from_pointee(None));
        // Overlay present but no name/url fields set.
        let overlay = Arc::new(ArcSwap::from_pointee(Some(SourceOverlay::default())));
        let frame = build_icy_frame(&info, &current_title, &overlay, &mut no_last_meta());
        let s = String::from_utf8_lossy(&frame[1..]);
        assert!(s.contains("StreamTitle='Cfg Name';"), "got: {s:?}");
        assert!(s.contains("StreamUrl='https://cfg';"), "got: {s:?}");
    }
}
