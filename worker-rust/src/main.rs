mod object_store;
mod query;

use std::env;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod dataforge {
    tonic::include_proto!("dataforge");
}

use dataforge::coordinator_service_client::CoordinatorServiceClient;
use dataforge::worker_service_server::{WorkerService, WorkerServiceServer};
use dataforge::{
    HeartbeatRequest, PartialResult, PingRequest, PingResponse, StreamBatchRequest,
    StreamBatchResponse, TaskRequest, WorkerInfo,
};

#[derive(Clone)]
struct Worker {
    active_tasks: Arc<AtomicI32>,
}

fn process_stream_micro_batch(request: StreamBatchRequest) -> Result<StreamBatchResponse, String> {
    let config: Value = serde_json::from_str(&request.config_json)
        .map_err(|error| format!("invalid stream config: {error}"))?;
    let events: Vec<Value> = serde_json::from_str(&request.events_json)
        .map_err(|error| format!("invalid stream events: {error}"))?;
    if events.len() > 10_000 {
        return Err("stream micro-batch must contain at most 10000 events".to_owned());
    }
    let event_time_column = config
        .get("eventTimeColumn")
        .and_then(Value::as_str)
        .ok_or_else(|| "eventTimeColumn is required".to_owned())?;
    let window_type = config
        .get("windowType")
        .and_then(Value::as_str)
        .ok_or_else(|| "windowType is required".to_owned())?;
    let size_ms = config
        .get("sizeMs")
        .and_then(Value::as_i64)
        .ok_or_else(|| "sizeMs is required".to_owned())?;
    let slide_ms = config
        .get("slideMs")
        .and_then(Value::as_i64)
        .unwrap_or(size_ms);
    if size_ms <= 0 || slide_ms <= 0 || !matches!(window_type, "TUMBLE" | "HOP" | "SESSION") {
        return Err("invalid stream window configuration".to_owned());
    }
    let mut records = Vec::with_capacity(events.len());
    for event in events {
        let timestamp = event
            .get(event_time_column)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("invalid event time in {event_time_column}"))?;
        let event_time = chrono::DateTime::parse_from_rfc3339(timestamp)
            .map_err(|_| format!("invalid event time in {event_time_column}"))?
            .timestamp_millis();
        let mut windows = Vec::new();
        if window_type == "TUMBLE" {
            let start = event_time.div_euclid(size_ms) * size_ms;
            windows.push(json!({ "start": start, "end": start + size_ms }));
        } else if window_type == "HOP" {
            let mut start = event_time.div_euclid(slide_ms) * slide_ms;
            while start > event_time - size_ms {
                windows.push(json!({ "start": start, "end": start + size_ms }));
                start -= slide_ms;
            }
        }
        records.push(json!({ "event": event, "eventTime": event_time, "windows": windows }));
    }
    Ok(StreamBatchResponse {
        batch_id: request.batch_id,
        event_count: records.len() as i32,
        records_json: serde_json::to_string(&records)
            .map_err(|error| format!("could not encode stream records: {error}"))?,
    })
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    fn request(window_type: &str, size_ms: i64, slide_ms: i64) -> StreamBatchRequest {
        StreamBatchRequest {
            batch_id: "batch-1".to_owned(),
            config_json: json!({
                "eventTimeColumn": "occurred_at",
                "windowType": window_type,
                "sizeMs": size_ms,
                "slideMs": slide_ms
            })
            .to_string(),
            events_json: json!([{"occurred_at": "2026-10-02T00:00:07Z", "value": 3}]).to_string(),
        }
    }

    #[test]
    fn assigns_an_event_to_every_overlapping_hop_window() {
        let response = process_stream_micro_batch(request("HOP", 10_000, 5_000)).unwrap();
        let records: Vec<Value> = serde_json::from_str(&response.records_json).unwrap();
        assert_eq!(response.event_count, 1);
        assert_eq!(records[0]["windows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn rejects_invalid_event_time_without_partial_output() {
        let mut invalid = request("TUMBLE", 10_000, 10_000);
        invalid.events_json = json!([{"occurred_at": "not-a-timestamp"}]).to_string();
        assert!(process_stream_micro_batch(invalid).is_err());
    }
}

#[tonic::async_trait]
impl WorkerService for Worker {
    type ExecuteTaskStream =
        Pin<Box<dyn Stream<Item = Result<PartialResult, Status>> + Send + 'static>>;

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let task = request.into_inner();
        let (sender, receiver) = mpsc::channel(2);
        let active_tasks = self.active_tasks.clone();
        active_tasks.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let task_id = task.task_id.clone();
            if let Err(error) = query::execute(task, sender.clone()).await {
                let _ = sender
                    .send(Err(Status::internal(format!(
                        "task {task_id} failed: {error}"
                    ))))
                    .await;
            }
            active_tasks.fetch_sub(1, Ordering::Relaxed);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            alive: true,
            cache_stats_json: "{}".to_string(),
        }))
    }

    async fn process_stream_batch(
        &self,
        request: Request<StreamBatchRequest>,
    ) -> Result<Response<StreamBatchResponse>, Status> {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
        let result = process_stream_micro_batch(request.into_inner());
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        result.map(Response::new).map_err(Status::invalid_argument)
    }
}

async fn register(
    coordinator: &str,
    worker_id: &str,
    worker_address: &str,
    worker_port: i32,
) -> Result<CoordinatorServiceClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    let mut last_error = None;
    for attempt in 1..=20 {
        match CoordinatorServiceClient::connect(format!("http://{coordinator}")).await {
            Ok(mut client) => match client
                .register(WorkerInfo {
                    worker_id: worker_id.to_owned(),
                    address: worker_address.to_owned(),
                    port: worker_port,
                    capabilities: vec![
                        "scan".to_owned(),
                        "aggregate".to_owned(),
                        "join".to_owned(),
                        "stream".to_owned(),
                    ],
                })
                .await
            {
                Ok(response) => {
                    if response.into_inner().success {
                        return Ok(client);
                    }
                    last_error = Some("coordinator rejected registration".to_owned());
                }
                Err(error) => last_error = Some(error.to_string()),
            },
            Err(error) => last_error = Some(error.to_string()),
        }
        eprintln!("registration attempt {attempt}/20 failed; retrying");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Err(last_error
        .unwrap_or_else(|| "registration failed".to_owned())
        .into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = env::var("WORKER_ID").unwrap_or_else(|_| "worker-rust".to_owned());
    let worker_address = env::var("WORKER_ADDRESS").unwrap_or_else(|_| worker_id.clone());
    let worker_port: i32 = env::var("WORKER_PORT")
        .unwrap_or_else(|_| "50051".to_owned())
        .parse()?;
    let coordinator =
        env::var("COORDINATOR_ADDRESS").unwrap_or_else(|_| "coordinator:50050".to_owned());
    let listen_address = format!("0.0.0.0:{worker_port}").parse()?;
    let active_tasks = Arc::new(AtomicI32::new(0));
    let worker = Worker {
        active_tasks: active_tasks.clone(),
    };

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(worker))
            .serve(listen_address)
            .await
    });

    let mut coordinator_client =
        register(&coordinator, &worker_id, &worker_address, worker_port).await?;
    println!("{worker_id} registered and ready on {worker_address}:{worker_port}");

    let heartbeat_id = worker_id.clone();
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(5));
        loop {
            timer.tick().await;
            let response = coordinator_client
                .heartbeat(HeartbeatRequest {
                    worker_id: heartbeat_id.clone(),
                    status: "alive".to_owned(),
                    active_tasks: active_tasks.load(Ordering::Relaxed),
                })
                .await;
            if let Err(error) = response {
                eprintln!("heartbeat failed: {error}");
            }
        }
    });

    tokio::select! {
        result = server => result??,
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}
