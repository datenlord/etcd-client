//! Leases are a mechanism for detecting client liveness. The cluster grants leases with a time-to-live. A lease expires if the etcd cluster does not receive a keepAlive within a given TTL period.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use smol::channel::{unbounded, Receiver, Sender};
use smol::stream::Stream;
use tonic::transport::Channel;

pub use grant::{LeaseGrantRequest, LeaseGrantResponse};
pub use keep_alive::{LeaseKeepAliveRequest, LeaseKeepAliveResponse};
pub use revoke::{LeaseRevokeRequest, LeaseRevokeResponse};

use crate::lazy::{Lazy, Shutdown};
use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::lease_client::LeaseClient;
use crate::{Error, Result};

mod grant;
mod keep_alive;
mod revoke;

/// LeaseKeepAliveTunnel is a reusable connection for `Lease Keep Alive` operation.
/// The underlying gRPC method is Bi-directional streaming.
struct LeaseKeepAliveTunnel {
    req_sender: Sender<LeaseKeepAliveRequest>,
    resp_receiver: Option<Receiver<Result<LeaseKeepAliveResponse>>>,
    shutdown: Option<Sender<()>>,
}

impl LeaseKeepAliveTunnel {
    fn new(mut client: LeaseClient<Channel>) -> Self {
        let (req_sender, req_receiver) = unbounded::<LeaseKeepAliveRequest>();
        let (resp_sender, resp_receiver) = unbounded::<Result<LeaseKeepAliveResponse>>();

        let (shutdown_tx, shutdown_rx) = unbounded();

        let request = tonic::Request::new(async_stream::stream! {
            while let Ok(req) = req_receiver.recv().await {
                let pb: etcdserverpb::LeaseKeepAliveRequest = req.into();
                yield pb;
            }
        });

        // monitor inbound watch response and transfer to the receiver
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx.into_future().fuse();
            let mut inbound = futures::select! {
                res = client.lease_keep_alive(request).fuse() => res.unwrap().into_inner(),
                _ = shutdown_rx => { return; }
            };

            loop {
                let resp = futures::select! {
                    resp = inbound.message().fuse() => resp,
                    _ = shutdown_rx => { return; }
                };
                match resp {
                    Ok(Some(resp)) => {
                        resp_sender
                            .send(Ok(From::from(resp)))
                            .await
                            .unwrap_or_else(|_| panic!());
                    }
                    Ok(None) => {
                        return;
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

    fn take_resp_receiver(&mut self) -> Receiver<Result<LeaseKeepAliveResponse>> {
        self.resp_receiver
            .take()
            .unwrap_or_else(|| panic!("failed fetch LeaseKeepAliveResponse"))
    }
}

#[async_trait]
impl Shutdown for LeaseKeepAliveTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).await.map_err(|_| Error::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Lease client.
#[derive(Clone)]
pub struct Lease {
    client: LeaseClient<Channel>,
    keep_alive_tunnel: Arc<Lazy<LeaseKeepAliveTunnel>>,
}

impl Lease {
    pub(crate) fn new(client: LeaseClient<Channel>) -> Self {
        let keep_alive_tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || LeaseKeepAliveTunnel::new(client.clone())))
        };
        Self {
            client,
            keep_alive_tunnel,
        }
    }

    /// Performs a lease granting operation.
    pub async fn grant(&mut self, req: LeaseGrantRequest) -> Result<LeaseGrantResponse> {
        let resp = self
            .client
            .lease_grant(tonic::Request::new(req.into()))
            .await?;

        Ok(From::from(resp.into_inner()))
    }

    /// Performs a lease revoking operation.
    pub async fn revoke(&mut self, req: LeaseRevokeRequest) -> Result<LeaseRevokeResponse> {
        let resp = self
            .client
            .lease_revoke(tonic::Request::new(req.into()))
            .await?;

        Ok(From::from(resp.into_inner()))
    }

    /// Fetch keep alive response stream.
    pub async fn keep_alive_responses(
        &mut self,
    ) -> impl Stream<Item = Result<LeaseKeepAliveResponse>> {
        self.keep_alive_tunnel.write().await.take_resp_receiver()
    }

    /// Performs a lease refreshing operation.
    pub async fn keep_alive(&mut self, req: LeaseKeepAliveRequest) -> Result<()> {
        self.keep_alive_tunnel
            .write()
            .await
            .req_sender
            .send(req)
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Shut down the running lease task, if any.
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.keep_alive_tunnel.evict().await
    }
}
