//! The Watch API provides an event-based interface for asynchronously monitoring changes to keys.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;

use futures::stream::StreamExt;
use smol::channel::{unbounded, Receiver, Sender};
use smol::stream::Stream;
use tonic::transport::Channel;

pub use watch_impl::{WatchRequest, WatchResponse};

use crate::lazy::{Lazy, Shutdown};
use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_client::WatchClient;
use crate::proto::mvccpb;
use crate::KeyValue;
use crate::Result;
use crate::{Error, KeyRange};

mod watch_impl;

/// WatchTunnel is a reusable connection for `Watch` operation
/// The underlying gRPC method is Bi-directional streaming
struct WatchTunnel {
    req_sender: Sender<WatchRequest>,
    resp_receiver: Option<Receiver<Result<WatchResponse>>>,
    shutdown: Option<Sender<()>>,
}

impl WatchTunnel {
    fn new(mut client: WatchClient<Channel>) -> Self {
        let (req_sender, req_receiver) = unbounded::<WatchRequest>();
        let (resp_sender, resp_receiver) = unbounded::<Result<WatchResponse>>();

        let request = tonic::Request::new(async_stream::stream! {
            while let Ok(req) = req_receiver.recv().await {
                let pb: etcdserverpb::WatchRequest = req.into();
                yield pb;
            }
        });

        let (shutdown_tx, shutdown_rx) = unbounded();

        // monitor inbound watch response and transfer to the receiver
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx.into_future().fuse();
            let mut inbound = futures::select! {
                res = client.watch(request).fuse() => res.unwrap().into_inner(),
                _ = shutdown_rx => { return; },
            };

            loop {
                let resp = futures::select! {
                    resp = inbound.message().fuse() => resp,
                    _ = shutdown_rx => { return; },
                };
                match resp {
                    Ok(Some(resp)) => {
                        resp_sender.send(Ok(From::from(resp))).await.unwrap();
                    }
                    Ok(None) => {
                        return;
                    }
                    Err(e) => {
                        resp_sender.send(Err(From::from(e))).await.unwrap();
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

    fn take_resp_receiver(&mut self) -> Receiver<Result<WatchResponse>> {
        self.resp_receiver.take().unwrap()
    }
}

#[async_trait]
impl Shutdown for WatchTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).await.map_err(|_| Error::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Watch client.
#[derive(Clone)]
pub struct Watch {
    client: WatchClient<Channel>,
    tunnel: Arc<Lazy<WatchTunnel>>,
}

impl Watch {
    pub(crate) fn new(client: WatchClient<Channel>) -> Self {
        let tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || WatchTunnel::new(client.clone())))
        };

        Self { client, tunnel }
    }

    /// Performs a watch operation.
    pub async fn watch(
        &mut self,
        key_range: KeyRange,
    ) -> impl Stream<Item = Result<WatchResponse>> {
        let mut tunnel = self.tunnel.write().await;
        tunnel
            .req_sender
            .send(WatchRequest::create(key_range))
            .await
            .expect("emit watch request");
        tunnel.take_resp_receiver()
    }

    /// Shut down the running watch task, if any.
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.tunnel.evict().await
    }
}

/// The kind of event.
pub enum EventType {
    Put,
    Delete,
}

impl From<mvccpb::event::EventType> for EventType {
    fn from(event_type: mvccpb::event::EventType) -> Self {
        use mvccpb::event::EventType;
        match event_type {
            EventType::Put => Self::Put,
            EventType::Delete => Self::Delete,
        }
    }
}

/// Every change to every key is represented with Event messages.
pub struct Event {
    proto: mvccpb::Event,
}

impl Event {
    /// Gets the kind of event.
    pub fn event_type(&self) -> EventType {
        match self.proto.r#type {
            0 => EventType::Put,
            _ => EventType::Delete, // FIXME: assert valid event type
        }
    }

    /// Takes the key-value pair out of response, leaving a `None` in its place.
    pub fn take_kvs(&mut self) -> Option<KeyValue> {
        match self.proto.kv.take() {
            Some(kv) => Some(From::from(kv)),
            _ => None,
        }
    }
}

impl From<mvccpb::Event> for Event {
    fn from(event: mvccpb::Event) -> Self {
        Self { proto: event }
    }
}
