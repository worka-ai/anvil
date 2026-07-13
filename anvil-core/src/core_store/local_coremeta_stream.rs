use super::*;
use crate::anvil_api::{
    CoreMetaStreamRequest, CoreMetaStreamResponse,
    core_meta_replication_internal_client::CoreMetaReplicationInternalClient,
};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;

type PendingCoreMetaStreamResponse =
    oneshot::Sender<std::result::Result<CoreMetaStreamResponse, CoreMetaStreamFailure>>;

#[derive(Debug, Clone)]
pub(super) struct CoreMetaPeerStream {
    sender: mpsc::Sender<CoreMetaStreamRequest>,
    pending: Arc<Mutex<BTreeMap<String, PendingCoreMetaStreamResponse>>>,
    closed: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub(super) struct CoreMetaStreamFailure {
    code: tonic::Code,
    message: String,
}

impl CoreMetaStreamFailure {
    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::Unavailable,
            message: message.into(),
        }
    }

    fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::DeadlineExceeded,
            message: message.into(),
        }
    }

    fn from_status(status: tonic::Status) -> Self {
        Self {
            code: status.code(),
            message: status.message().to_string(),
        }
    }

    fn to_status(&self) -> tonic::Status {
        tonic::Status::new(self.code, self.message.clone())
    }
}

impl CoreMetaPeerStream {
    pub(super) fn is_open(&self) -> bool {
        !self.closed.load(Ordering::SeqCst) && !self.sender.is_closed()
    }

    async fn request(
        &self,
        frame: CoreMetaStreamRequest,
    ) -> std::result::Result<CoreMetaStreamResponse, CoreMetaStreamFailure> {
        if !self.is_open() {
            return Err(CoreMetaStreamFailure::unavailable(
                "CoreMeta peer stream is closed",
            ));
        }
        let request_id = frame.request_id.clone();
        if request_id.trim().is_empty() {
            return Err(CoreMetaStreamFailure {
                code: tonic::Code::InvalidArgument,
                message: "CoreMeta stream request_id is required".to_string(),
            });
        }

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id.clone(), tx);
        if self.sender.send(frame).await.is_err() {
            self.pending.lock().await.remove(&request_id);
            self.closed.store(true, Ordering::SeqCst);
            return Err(CoreMetaStreamFailure::unavailable(
                "CoreMeta peer stream sender closed",
            ));
        }

        match tokio::time::timeout(CORE_INTERNAL_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.closed.store(true, Ordering::SeqCst);
                Err(CoreMetaStreamFailure::unavailable(
                    "CoreMeta peer stream response dispatcher closed",
                ))
            }
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                Err(CoreMetaStreamFailure::deadline_exceeded(format!(
                    "CoreMeta peer stream request {request_id} timed out"
                )))
            }
        }
    }
}

impl CoreStore {
    pub(super) async fn coremeta_stream_request(
        &self,
        public_api_addr: &str,
        bearer: &str,
        operation_label: &str,
        mut frame: CoreMetaStreamRequest,
    ) -> Result<CoreMetaStreamResponse> {
        let total_started_at = Instant::now();
        let endpoint = normalise_grpc_endpoint(public_api_addr)?;
        if frame.request_id.trim().is_empty() {
            frame.request_id = uuid::Uuid::new_v4().to_string();
        }

        let mut failures = Vec::new();
        for attempt in 0..CORE_INTERNAL_REQUEST_ATTEMPTS {
            let stream_started_at = Instant::now();
            let stream = match self
                .coremeta_peer_stream(&endpoint, bearer, operation_label)
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    failures.push(format!("stream attempt {}: {error}", attempt + 1));
                    self.coremeta_streams.lock().await.remove(&endpoint);
                    if attempt + 1 < CORE_INTERNAL_REQUEST_ATTEMPTS {
                        tokio::time::sleep(core_internal_retry_delay(attempt)).await;
                        continue;
                    }
                    break;
                }
            };
            crate::emit_test_timing(
                format!("coremeta.internal.stream.client {operation_label} stream"),
                stream_started_at.elapsed(),
            );

            let call_started_at = Instant::now();
            match stream.request(frame.clone()).await {
                Ok(response) => {
                    crate::emit_test_timing(
                        format!("coremeta.internal.stream.client {operation_label} call"),
                        call_started_at.elapsed(),
                    );
                    crate::emit_test_timing(
                        format!("coremeta.internal.stream.client {operation_label} total"),
                        total_started_at.elapsed(),
                    );
                    return Ok(response);
                }
                Err(failure) if retryable_internal_failure(&failure) => {
                    failures.push(format!(
                        "stream request attempt {}: code={:?} message={}",
                        attempt + 1,
                        failure.code,
                        failure.message
                    ));
                    self.coremeta_streams.lock().await.remove(&endpoint);
                    if attempt + 1 < CORE_INTERNAL_REQUEST_ATTEMPTS {
                        tokio::time::sleep(core_internal_retry_delay(attempt)).await;
                    }
                }
                Err(failure) => {
                    self.coremeta_streams.lock().await.remove(&endpoint);
                    let status = failure.to_status();
                    return Err(anyhow!(
                        "{operation_label} stream request to {endpoint} failed: code={:?} message={}",
                        status.code(),
                        status.message()
                    ));
                }
            }
        }

        bail!(
            "{operation_label} stream request to {endpoint} failed after {CORE_INTERNAL_REQUEST_ATTEMPTS} attempts: {}",
            failures.join("; ")
        )
    }

    async fn coremeta_peer_stream(
        &self,
        endpoint: &str,
        bearer: &str,
        operation_label: &str,
    ) -> Result<CoreMetaPeerStream> {
        {
            let mut streams = self.coremeta_streams.lock().await;
            if let Some(stream) = streams.get(endpoint) {
                if stream.is_open() {
                    return Ok(stream.clone());
                }
            }
            streams.remove(endpoint);
        }

        let stream = self
            .open_coremeta_peer_stream(endpoint, bearer, operation_label)
            .await?;
        let mut streams = self.coremeta_streams.lock().await;
        Ok(streams
            .entry(endpoint.to_string())
            .or_insert_with(|| stream.clone())
            .clone())
    }

    async fn open_coremeta_peer_stream(
        &self,
        endpoint: &str,
        bearer: &str,
        operation_label: &str,
    ) -> Result<CoreMetaPeerStream> {
        let channel = Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(CORE_INTERNAL_CONNECT_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect {operation_label} CoreMeta stream at {endpoint}"))?;
        let (tx, rx) = mpsc::channel::<CoreMetaStreamRequest>(64);
        let pending = Arc::new(Mutex::new(
            BTreeMap::<String, PendingCoreMetaStreamResponse>::new(),
        ));
        let closed = Arc::new(AtomicBool::new(false));

        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreMeta internal bearer token")?;
        let mut client = CoreMetaReplicationInternalClient::new(channel);
        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        request
            .metadata_mut()
            .insert("authorization", authorization);
        let mut responses = client
            .core_meta_stream(request)
            .await
            .map(tonic::Response::into_inner)
            .with_context(|| format!("open {operation_label} CoreMeta stream at {endpoint}"))?;

        let pending_for_task = pending.clone();
        let closed_for_task = closed.clone();
        let endpoint_for_task = endpoint.to_string();
        tokio::spawn(async move {
            while let Some(item) = responses.next().await {
                match item {
                    Ok(response) => {
                        let tx = pending_for_task.lock().await.remove(&response.request_id);
                        if let Some(tx) = tx {
                            let _ = tx.send(Ok(response));
                        }
                    }
                    Err(status) => {
                        closed_for_task.store(true, Ordering::SeqCst);
                        drain_pending_coremeta_stream_responses(
                            &pending_for_task,
                            CoreMetaStreamFailure::from_status(status),
                        )
                        .await;
                        return;
                    }
                }
            }

            closed_for_task.store(true, Ordering::SeqCst);
            drain_pending_coremeta_stream_responses(
                &pending_for_task,
                CoreMetaStreamFailure::unavailable(format!(
                    "CoreMeta stream at {endpoint_for_task} closed"
                )),
            )
            .await;
        });

        Ok(CoreMetaPeerStream {
            sender: tx,
            pending,
            closed,
        })
    }
}

async fn drain_pending_coremeta_stream_responses(
    pending: &Arc<Mutex<BTreeMap<String, PendingCoreMetaStreamResponse>>>,
    failure: CoreMetaStreamFailure,
) {
    let mut pending = pending.lock().await;
    let responses = std::mem::take(&mut *pending);
    drop(pending);
    for (_, tx) in responses {
        let _ = tx.send(Err(failure.clone()));
    }
}

fn retryable_internal_failure(failure: &CoreMetaStreamFailure) -> bool {
    matches!(
        failure.code,
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || (failure.code == tonic::Code::Unknown
        && ["transport", "service was not ready", "connection"]
            .iter()
            .any(|needle| failure.message.to_ascii_lowercase().contains(needle)))
}
