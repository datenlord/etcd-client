//! Leases are a mechanism for detecting client liveness.
//! The cluster grants leases with a time-to-live.
//! A lease expires if the etcd cluster does not receive a keepAlive within a given TTL period.
//!
//! # Examples
//!
//! Grant lease and keep lease alive
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use etcd_client::*;
//!
//! fn main() -> Result<()> {
//!     smol::block_on(async {
//!     let config = ClientConfig::new(vec!["http://127.0.0.1:2379".to_owned()], None, 32, true);
//!     let client = Client::connect(config).await?;
//!
//!         let key = "foo";
//!
//!         // grant lease
//!         let lease = client
//!             .lease()
//!             .grant(EtcdLeaseGrantRequest::new(Duration::from_secs(3)))
//!             .await?;
//!
//!         let lease_id = lease.id();
//!
//!         // set key with lease
//!         client
//!             .kv()
//!             .put({
//!                 let mut req = EtcdPutRequest::new(key, "bar");
//!                 req.set_lease(lease_id);
//!
//!                 req
//!             })
//!             .await?;
//!
//!         {
//!             client
//!                 .lease()
//!                 .keep_alive(EtcdLeaseKeepAliveRequest::new(lease_id))
//!                 .await;
//!         }
//!
//!         // not necessary, but will cleanly shut down the long-running tasks
//!         // spawned by the client
//!         client.shutdown().await;
//!
//!         Ok(())
//!     })
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use futures::prelude::*;

use smol::channel::{unbounded, Receiver, Sender};
use smol::stream::Stream;

pub use grant::{EtcdLeaseGrantRequest, EtcdLeaseGrantResponse};
pub use keep_alive::{EtcdLeaseKeepAliveRequest, EtcdLeaseKeepAliveResponse};
pub use revoke::{EtcdLeaseRevokeRequest, EtcdLeaseRevokeResponse};

use crate::lazy::{Lazy, Shutdown};
use crate::protos::rpc;
use crate::protos::rpc_grpc::LeaseClient;
use crate::retryable;
use crate::Result;
use crate::CURRENT_INTERVAL_ENV_KEY;
use crate::CURRENT_INTERVAL_VALUE;
use crate::INITIAL_INTERVAL_ENV_KEY;
use crate::INITIAL_INTERVAL_VALUE;
use crate::MAX_ELAPSED_TIME_ENV_KEY;
use crate::MAX_ELAPSED_TIME_VALUE;
use backoff::ExponentialBackoff;
use std::time::Duration;

use grpcio::WriteFlags;

/// Grant mod for granting lease operations.
mod grant;
/// Keep alive mod for keeping lease operations.
mod keep_alive;
/// Revoke mod for revoking lease operations.
mod revoke;

/// `LeaseKeepAliveTunnel` is a reusable connection for `Lease Keep Alive` operation.
/// The underlying `gRPC` method is Bi-directional streaming.
struct LeaseKeepAliveTunnel {
    /// A channel sender to send keep alive request.
    req_sender: Sender<EtcdLeaseKeepAliveRequest>,
    /// A channel receiver to receive keep alive response.
    resp_receiver: Option<Receiver<Result<EtcdLeaseKeepAliveResponse>>>,
    /// A channel sender to send shutdown request.
    shutdown: Option<Sender<()>>,
}

impl LeaseKeepAliveTunnel {
    /// Creates a new `LeaseClient`.
    fn new(client: &LeaseClient) -> Self {
        let (req_sender, req_receiver) = unbounded::<EtcdLeaseKeepAliveRequest>();
        let (resp_sender, resp_receiver) = unbounded::<Result<EtcdLeaseKeepAliveResponse>>();

        let (shutdown_tx, shutdown_rx) = unbounded();
        let shutdown_reponse = shutdown_rx.clone();
        // Monitor inbound lease response and transfer to the receiver
        let (mut client_req_sender, mut client_resp_receiver) = client
            .lease_keep_alive()
            .unwrap_or_else(|e| panic!("Fail to lease_keep_alive, the error is: {}", e));
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx.into_future().fuse();
            #[allow(clippy::mut_mut)]
            while let Ok(req) = req_receiver.recv().await {
                let lease_keep_alive_request: rpc::LeaseKeepAliveRequest = req.into();

                futures::select! {
                    res = client_req_sender.send(
                        (lease_keep_alive_request, WriteFlags::default())
                    ).fuse() => res.unwrap_or_else(
                        |e| panic!("Fail to send request, the error is {}", e)
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
                    resp_opt = client_resp_receiver.next().fuse() => if let Some(resp)=resp_opt {
                        resp
                    } else {
                        return;
                    },
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

    /// Takes `resp_receiver`
    fn take_resp_receiver(&mut self) -> Receiver<Result<EtcdLeaseKeepAliveResponse>> {
        self.resp_receiver
            .take()
            .unwrap_or_else(|| panic!("failed fetch LeaseKeepAliveResponse"))
    }
}

#[async_trait]
impl Shutdown for LeaseKeepAliveTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).await?;
        }
        Ok(())
    }
}

/// Lease client.
#[derive(Clone)]
pub struct Lease {
    /// Etcd lease client provides lease related operations.
    client: LeaseClient,
    /// A tunnel used to communicate with Etcd server to keep lease alive.
    keep_alive_tunnel: Arc<Lazy<LeaseKeepAliveTunnel>>,
}

impl Lease {
    /// Creates a new `LeaseClient`.
    pub(crate) fn new(client: LeaseClient) -> Self {
        let keep_alive_tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || {
                LeaseKeepAliveTunnel::new(&client.clone())
            }))
        };
        Self {
            client,
            keep_alive_tunnel,
        }
    }

    /// Performs a lease granting operation.
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shut down.
    #[inline]
    pub async fn grant(&mut self, req: EtcdLeaseGrantRequest) -> Result<EtcdLeaseGrantResponse> {
        let resp = retryable!(|| async {
            let resp = self.client.lease_grant_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Performs a lease revoking operation.
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shut down.
    #[inline]
    pub async fn revoke(&mut self, req: EtcdLeaseRevokeRequest) -> Result<EtcdLeaseRevokeResponse> {
        let resp = retryable!(|| async {
            let resp = self.client.lease_revoke_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Fetches keep alive response stream.
    #[inline]
    pub async fn keep_alive_responses(
        &mut self,
    ) -> impl Stream<Item = Result<EtcdLeaseKeepAliveResponse>> {
        self.keep_alive_tunnel.write().await.take_resp_receiver()
    }

    /// Performs a lease refreshing operation.
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shut down.
    #[inline]
    pub async fn keep_alive(&mut self, req: EtcdLeaseKeepAliveRequest) -> Result<()> {
        self.keep_alive_tunnel
            .write()
            .await
            .req_sender
            .send(req)
            .await?;
        Ok(())
    }

    /// Shut down the running lease task, if any.
    ///
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shut down.
    #[inline]
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.keep_alive_tunnel.evict().await
    }
}
