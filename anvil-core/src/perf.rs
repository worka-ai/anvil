use std::fmt::Write as _;
use std::future::Future;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
struct PerfEvent {
    measurement: String,
    labels: Vec<(String, String)>,
    fields: Vec<(String, PerfField)>,
    timestamp_ns: u128,
}

#[derive(Debug, Clone)]
enum PerfField {
    I64(i64),
    U64(u64),
    U128(u128),
    F64(f64),
    Str(String),
}

#[derive(Clone)]
struct PerfSink {
    tx: mpsc::UnboundedSender<PerfMessage>,
}

static PERF_SINK: OnceLock<Option<PerfSink>> = OnceLock::new();

tokio::task_local! {
    static PERF_CONTEXT: Vec<(String, String)>;
}

enum PerfMessage {
    Event(PerfEvent),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct PerfGuard {
    measurement: &'static str,
    labels: Vec<(String, String)>,
    started_at: Instant,
}

pub fn enabled() -> bool {
    std::env::var_os("ANVIL_PERF_TRACE").is_some()
        || std::env::var_os("ANVIL_PERF_GREPTIME_URL").is_some()
        || std::env::var_os("ANVIL_GREPTIME_WRITE_URL").is_some()
        || std::env::var_os("ANVIL_PERF_TRACE_FILE").is_some()
}

pub fn guard(measurement: &'static str, labels: &[(&str, &str)]) -> PerfGuard {
    PerfGuard {
        measurement,
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        started_at: Instant::now(),
    }
}

pub fn record_duration(measurement: &str, labels: &[(&str, &str)], duration: Duration) {
    if !enabled() {
        return;
    }
    let fields = vec![
        (
            "duration_nanos".to_string(),
            PerfField::U128(duration.as_nanos()),
        ),
        (
            "duration_ms".to_string(),
            PerfField::F64(duration.as_secs_f64() * 1000.0),
        ),
    ];
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields,
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_io_duration(
    component: &str,
    operation: &str,
    path: &Path,
    bytes: u64,
    duration: Duration,
) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: "anvil_file_io".to_string(),
        labels: vec![
            ("component".to_string(), component.to_string()),
            ("operation".to_string(), operation.to_string()),
            ("file_path".to_string(), path.to_string_lossy().into_owned()),
        ],
        fields: vec![
            (
                "duration_nanos".to_string(),
                PerfField::U128(duration.as_nanos()),
            ),
            (
                "duration_ms".to_string(),
                PerfField::F64(duration.as_secs_f64() * 1000.0),
            ),
            ("bytes".to_string(), PerfField::U64(bytes)),
        ],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_counter(measurement: &str, labels: &[(&str, &str)], value: u64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("count".to_string(), PerfField::U64(value))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_gauge(measurement: &str, labels: &[(&str, &str)], value: i64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("value".to_string(), PerfField::I64(value))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_text(measurement: &str, labels: &[(&str, &str)], field: &str, value: &str) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), ((*value).to_string())))
            .collect(),
        fields: vec![(field.to_string(), PerfField::Str(value.to_string()))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub async fn flush() {
    if let Some(sink) = PERF_SINK.get().and_then(|sink| sink.as_ref()) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = sink.tx.send(PerfMessage::Flush(ack_tx));
        let _ = tokio::time::timeout(Duration::from_secs(5), ack_rx).await;
    }
}

pub async fn with_context<F, T>(labels: Vec<(String, String)>, future: F) -> T
where
    F: Future<Output = T>,
{
    let mut combined = PERF_CONTEXT.try_with(Clone::clone).unwrap_or_default();
    merge_labels(&mut combined, labels);
    PERF_CONTEXT.scope(combined, future).await
}

impl Drop for PerfGuard {
    fn drop(&mut self) {
        let labels = self
            .labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        record_duration(self.measurement, &labels, self.started_at.elapsed());
    }
}

fn emit(mut event: PerfEvent) {
    let Some(sink) = PERF_SINK.get_or_init(init_sink).as_ref() else {
        return;
    };
    event.labels = labels_with_context(event.labels);
    let _ = sink.tx.send(PerfMessage::Event(event));
}

fn labels_with_context(mut labels: Vec<(String, String)>) -> Vec<(String, String)> {
    if let Ok(context) = PERF_CONTEXT.try_with(Clone::clone) {
        for (key, value) in context {
            if !labels.iter().any(|(existing, _)| existing == &key) {
                labels.push((key, value));
            }
        }
    }
    labels
}

fn merge_labels(target: &mut Vec<(String, String)>, labels: Vec<(String, String)>) {
    for (key, value) in labels {
        if let Some((_, existing)) = target
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            *existing = value;
        } else {
            target.push((key, value));
        }
    }
}

fn init_sink() -> Option<PerfSink> {
    let greptime_url = std::env::var("ANVIL_PERF_GREPTIME_URL")
        .ok()
        .or_else(|| std::env::var("ANVIL_GREPTIME_WRITE_URL").ok());
    let file_path = std::env::var("ANVIL_PERF_TRACE_FILE").ok();
    if greptime_url.is_none() && file_path.is_none() {
        return None;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return None;
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<PerfMessage>();
    handle.spawn(async move {
        let client = reqwest::Client::new();
        let mut pending = Vec::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(PerfMessage::Event(event)) => {
                            pending.push(event);
                            if pending.len() >= 100 {
                                flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            }
                        }
                        Some(PerfMessage::Flush(ack)) => {
                            flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            let _ = ack.send(());
                        }
                        None => {
                            flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                }
            }
        }
    });
    Some(PerfSink { tx })
}

async fn flush_events(
    client: &reqwest::Client,
    greptime_url: Option<&str>,
    file_path: Option<&str>,
    pending: &mut Vec<PerfEvent>,
) {
    if pending.is_empty() {
        return;
    }
    let mut body = String::new();
    for event in pending.drain(..) {
        body.push_str(&to_line_protocol(&event));
        body.push('\n');
    }
    if let Some(path) = file_path {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Ok(mut file) = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            use tokio::io::AsyncWriteExt;
            let _ = file.write_all(body.as_bytes()).await;
        }
    }
    if let Some(url) = greptime_url {
        let _ = client
            .post(url)
            .header("content-type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await;
    }
}

fn to_line_protocol(event: &PerfEvent) -> String {
    let mut line = String::new();
    line.push_str(&escape_key(&event.measurement));
    for (key, value) in &event.labels {
        let _ = write!(line, ",{}={}", escape_key(key), escape_key(value));
    }
    line.push(' ');
    for (idx, (key, value)) in event.fields.iter().enumerate() {
        if idx > 0 {
            line.push(',');
        }
        let _ = write!(line, "{}={}", escape_key(key), format_field(value));
    }
    let _ = write!(line, " {}", event.timestamp_ns);
    line
}

fn format_field(value: &PerfField) -> String {
    match value {
        PerfField::I64(value) => format!("{value}i"),
        PerfField::U64(value) => format!("{}i", (*value).min(i64::MAX as u64)),
        PerfField::U128(value) => format!("{}i", (*value).min(i64::MAX as u128)),
        PerfField::F64(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "0".to_string()
            }
        }
        PerfField::Str(value) => {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
        }
    }
}

fn escape_key(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_protocol_escapes_tags_and_fields() {
        let event = PerfEvent {
            measurement: "anvil perf".to_string(),
            labels: vec![("path".to_string(), "/a,b=c".to_string())],
            fields: vec![
                ("duration_nanos".to_string(), PerfField::U64(42)),
                (
                    "note".to_string(),
                    PerfField::Str("a \"quoted\" value".to_string()),
                ),
            ],
            timestamp_ns: 7,
        };
        let line = to_line_protocol(&event);
        assert!(line.starts_with("anvil\\ perf,path=/a\\,b\\=c "));
        assert!(line.contains("duration_nanos=42i"));
        assert!(line.contains("note=\"a \\\"quoted\\\" value\""));
        assert!(line.ends_with(" 7"));
    }

    #[tokio::test]
    async fn context_labels_are_added_without_overriding_event_labels() {
        let event = with_context(
            vec![
                ("request_id".to_string(), "req-1".to_string()),
                ("path".to_string(), "/outer".to_string()),
            ],
            async {
                PerfEvent {
                    measurement: "anvil_request".to_string(),
                    labels: labels_with_context(vec![("path".to_string(), "/event".to_string())]),
                    fields: vec![("duration_nanos".to_string(), PerfField::U64(42))],
                    timestamp_ns: 7,
                }
            },
        )
        .await;

        assert!(
            event
                .labels
                .contains(&("request_id".to_string(), "req-1".to_string()))
        );
        assert!(
            event
                .labels
                .contains(&("path".to_string(), "/event".to_string()))
        );
        assert!(
            !event
                .labels
                .contains(&("path".to_string(), "/outer".to_string()))
        );
    }
}
