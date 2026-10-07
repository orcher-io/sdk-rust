//! A worker that shuts down gracefully deregisters itself.
//!
//! Without `DeregisterWorker` the server keeps listing a stopped worker as
//! connected until its registration expires. The worker runs here against a
//! real gRPC server that serves only `WorkerService`; the polls it also makes
//! are refused as unimplemented, which the worker tolerates.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orcher_sdk::worker::WorkerBuilder;
use orcher_sdk_core::proto::orcher::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const REGISTRATION_ID: &str = "reg-under-test";

#[derive(Default)]
struct Recorded {
    registered: Vec<RegisterWorkerRequest>,
    deregistered: Vec<DeregisterWorkerRequest>,
}

/// Accepts registration and heartbeats, and records each deregistration.
/// With `hang_on_deregister` it never answers `DeregisterWorker`.
#[derive(Clone, Default)]
struct FakeWorkerService {
    recorded: Arc<Mutex<Recorded>>,
    hang_on_deregister: bool,
}

#[tonic::async_trait]
impl WorkerService for FakeWorkerService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        self.recorded
            .lock()
            .unwrap()
            .registered
            .push(request.into_inner());
        Ok(Response::new(RegisterWorkerResponse {
            success: true,
            registration_id: REGISTRATION_ID.to_string(),
            error_message: String::new(),
        }))
    }

    async fn worker_heartbeat(
        &self,
        _: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
        Ok(Response::new(WorkerHeartbeatResponse {
            success: true,
            re_register: false,
            timestamp: 0,
        }))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        self.recorded
            .lock()
            .unwrap()
            .deregistered
            .push(request.into_inner());
        if self.hang_on_deregister {
            std::future::pending::<()>().await;
        }
        Ok(Response::new(DeregisterWorkerResponse { success: true }))
    }
}

/// Starts a worker against a fake server, waits until it has registered, asks
/// it to shut down and waits for `run` to return. Returns what the server saw
/// and how long shutdown took.
async fn run_and_shut_down(hang_on_deregister: bool) -> (Arc<Mutex<Recorded>>, Duration) {
    let service = FakeWorkerService {
        hang_on_deregister,
        ..Default::default()
    };
    let recorded = service.recorded.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let worker = Arc::new(
        WorkerBuilder::new()
            .server_url(format!("http://{addr}"))
            .task_queue("deregistration-test")
            // No heartbeat falls due while the test runs.
            .heartbeat_interval(Duration::from_secs(3600))
            .build()
            .await
            .unwrap(),
    );
    let running = tokio::spawn({
        let worker = worker.clone();
        async move { worker.run().await }
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        while recorded.lock().unwrap().registered.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker never registered");

    let asked = Instant::now();
    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(30), running)
        .await
        .expect("the worker did not stop after shutdown")
        .unwrap()
        .unwrap();
    (recorded, asked.elapsed())
}

#[tokio::test]
async fn a_worker_deregisters_when_it_shuts_down() {
    let (recorded, _) = run_and_shut_down(false).await;

    let recorded = recorded.lock().unwrap();
    let service_id = recorded.registered[0].service_id.clone();
    assert_eq!(
        recorded
            .deregistered
            .iter()
            .map(|r| (r.service_id.clone(), r.registration_id.clone()))
            .collect::<Vec<_>>(),
        [(service_id, REGISTRATION_ID.to_string())],
        "the worker did not deregister its registration on shutdown"
    );
}

#[tokio::test]
async fn a_server_that_never_answers_deregistration_does_not_hold_up_shutdown() {
    let (recorded, took) = run_and_shut_down(true).await;

    assert_eq!(recorded.lock().unwrap().deregistered.len(), 1);
    // Five seconds for deregistration, plus the time the work drivers take to
    // stop against a server that does not serve them.
    assert!(
        took < Duration::from_secs(20),
        "shutdown waited {took:?} on a deregistration that never answers"
    );
}
