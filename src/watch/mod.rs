//! The Watch API provides an event-based interface for asynchronously monitoring changes to keys.
//!
//! # Examples
//!
//! Watch key `foo` changes
//!
//! ```no_run
//!
//! use etcd_client::*;
//! use futures::stream::StreamExt;
//!
//! fn main() -> Result<()> {
//!     smol::block_on(async {
//!         let client = Client::connect(ClientConfig {
//!             endpoints: vec!["http://127.0.0.1:2379".to_owned()],
//!             auth: None,
//!             cache_size: 32,
//!             cache_enable: true,
//!         }).await?;
//!
//!         // print out all received watch responses
//!         let mut inbound = client.watch(KeyRange::key("foo")).await;
//!         smol::spawn(async move {
//!             while let Some(resp) = inbound.next().await {
//!                 println!("watch response: {:?}", resp);
//!             }
//!         });
//!
//!         let key = "foo";
//!         client.kv().put(EtcdPutRequest::new(key, "bar")).await?;
//!         client.kv().put(EtcdPutRequest::new(key, "baz")).await?;
//!         client
//!             .kv()
//!             .delete(EtcdDeleteRequest::new(KeyRange::key(key)))
//!             .await?;
//!
//!         // not necessary, but will cleanly shut down the long-running tasks
//!         // spawned by the client
//!         client.shutdown().await;
//!
//!         Ok(())
//!     })
//! }
//!
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;

use futures::stream::StreamExt;
use smol::channel::{unbounded, Receiver, Sender};
use smol::stream::Stream;

pub use watch_impl::{EtcdWatchRequest, EtcdWatchResponse};

use futures::prelude::*;

use crate::lazy::{Lazy, Shutdown};
use crate::protos::kv;
use crate::protos::rpc;
use crate::protos::rpc_grpc::WatchClient;
use crate::EtcdKeyValue;
use crate::KeyRange;
use crate::Result;
use grpcio::WriteFlags;

/// Watch implementation mod.
mod watch_impl;

/// `WatchTunnel` is a reusable connection for `Watch` operation
/// The underlying `gRPC` method is Bi-directional streaming
struct WatchTunnel {
    /// A channel sender to send watch request.
    req_sender: Sender<EtcdWatchRequest>,
    /// A channel receiver to receive watch response.
    resp_receiver: Option<Receiver<Result<EtcdWatchResponse>>>,
    /// A channel sender to send shutdowm request.
    shutdown: Option<Sender<()>>,
}

impl WatchTunnel {
    /// Creates a new `WatchClient`.
    fn new(client: &WatchClient) -> Self {
        let (req_sender, req_receiver) = unbounded::<EtcdWatchRequest>();
        let (resp_sender, resp_receiver) = unbounded::<Result<EtcdWatchResponse>>();

        let (shutdown_tx, shutdown_rx) = unbounded();
        let shutdown_reponse = shutdown_rx.clone();
        // Monitor inbound watch response and transfer to the receiver
        let (mut client_req_sender, mut client_resp_receiver) = client
            .watch()
            .unwrap_or_else(|e| panic!("failed to send watch commend, the error is: {}", e));
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx.into_future().fuse();
            #[allow(clippy::mut_mut)]
            while let Ok(req) = req_receiver.recv().await {
                let watch_request: rpc::WatchRequest = req.into();

                futures::select! {
                    res = client_req_sender.send(
                        (watch_request, WriteFlags::default())
                    ).fuse() => res.unwrap_or_else(
                        |e| panic!("Fail to send reqponse, the error is {}", e)
                    ),
                    _ = shutdown_rx => return
                };
            }
        })
        .detach();

        smol::spawn(async move {
            let mut shutdown_rx = shutdown_reponse.into_future().fuse();

            loop {
                #[allow(clippy::mut_mut)]
                let resp = futures::select! {
                    resp_opt = client_resp_receiver.next().fuse() => resp_opt.unwrap_or_else(
                        || panic!("Fail to receive reponse from client")
                    ),
                    _ = shutdown_rx => return
                };

                match resp {
                    Ok(resp) => {
                        resp_sender
                            .send(Ok(From::from(resp)))
                            .await
                            .unwrap_or_else(|e| {
                                panic!("failed to send response, the error is: {}", e)
                            });
                    }
                    Err(e) => {
                        resp_sender
                            .send(Err(From::from(e)))
                            .await
                            .unwrap_or_else(|e| {
                                panic!("failed to send response, the error is: {}", e)
                            });
                    }
                };
            }
        })
        .detach();

        Self {
            req_sender,
            resp_receiver: Some(resp_receiver),
            shutdown: Some(shutdown_tx),
        }
    }

    /// Takes resp receiver.
    fn take_resp_receiver(&mut self) -> Receiver<Result<EtcdWatchResponse>> {
        self.resp_receiver
            .take()
            .unwrap_or_else(|| panic!("Take resp_receiver error"))
    }
}

#[async_trait]
impl Shutdown for WatchTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).await?;
        }
        Ok(())
    }
}

/// Watch client.
#[derive(Clone)]
pub struct Watch {
    /// Etcd watch client provides watch realted operations.
    client: WatchClient,
    /// A tunnel used to communicate with Etcd server for watch operations.
    tunnel: Arc<Lazy<WatchTunnel>>,
}

impl Watch {
    /// Create a new `WatchClient`.
    pub(crate) fn new(client: WatchClient) -> Self {
        let tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || WatchTunnel::new(&client.clone())))
        };

        Self { client, tunnel }
    }

    /// Performs a watch operation.
    #[inline]
    pub async fn watch(
        &mut self,
        key_range: KeyRange,
    ) -> impl Stream<Item = Result<EtcdWatchResponse>> {
        let mut tunnel = self.tunnel.write().await;
        tunnel
            .req_sender
            .send(EtcdWatchRequest::create(key_range))
            .await
            .unwrap_or_else(|e| panic!("Fail to send watch request, the error is {}", e));
        tunnel.take_resp_receiver()
    }

    /// Shut down the running watch task, if any.
    ///
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shutdown.
    #[inline]
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.tunnel.evict().await
    }
}

/// The kind of event.
#[derive(Debug, Clone, Copy)]
pub enum EventType {
    /// Put event.
    Put,

    /// Delete event.
    Delete,
}

impl From<kv::Event_EventType> for EventType {
    #[inline]
    fn from(event_type: kv::Event_EventType) -> Self {
        match event_type {
            kv::Event_EventType::PUT => Self::Put,
            kv::Event_EventType::DELETE => Self::Delete,
        }
    }
}

/// Every change to every key is represented with Event messages.
#[derive(Debug)]
pub struct Event {
    /// Etcd event proto.
    proto: kv::Event,
}

impl Event {
    /// Takes the key-value pair out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_kvs(&mut self) -> Option<EtcdKeyValue> {
        match self.proto.kv.take() {
            Some(kv) => Some(From::from(kv)),
            None => None,
        }
    }
}

impl From<kv::Event> for Event {
    #[inline]
    fn from(event: kv::Event) -> Self {
        Self { proto: event }
    }
}
