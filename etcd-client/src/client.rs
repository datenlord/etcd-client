use std::sync::Arc;

use smol::stream::Stream;
use std::net::SocketAddr;

use grpcio::{Channel, ChannelBuilder, EnvBuilder, LbPolicy};

use crate::protos::{
    lock_grpc::LockClient,
    rpc_grpc::{AuthClient, KvClient, LeaseClient, WatchClient},
};
use crate::watch::EtcdWatchResponse;
use crate::{Auth, KeyRange, Kv, Lease, Lock, Result, Watch};

/// Config for establishing etcd client.
#[non_exhaustive]
pub struct ClientConfig {
    /// Etcd server end points.
    pub endpoints: Vec<String>,
    /// Etcd Auth configurations (User ID, password).
    pub auth: Option<(String, String)>,
    /// Etcd client cache size.
    pub cache_size: usize,
    /// Enable etcd client cache.
    pub cache_enable: bool,
}

impl ClientConfig {
    /// New a client config
    #[must_use]
    #[inline]
    pub fn new(
        endpoints: Vec<String>,
        auth: Option<(String, String)>,
        cache_size: usize,
        cache_enable: bool,
    ) -> Self {
        Self {
            endpoints,
            auth,
            cache_size,
            cache_enable,
        }
    }
}

/// Client is an abstraction for grouping etcd operations and managing underlying network communications.
#[derive(Clone)]
pub struct Client {
    /// Inner struct for etcd client.
    inner: Arc<Inner>,
}

#[allow(dead_code)]
/// Inner struct
pub struct Inner {
    /// A grpc channel used to communicate with Etcd server.
    channel: Channel,
    /// Auth client for authentication operations.
    auth_client: Auth,
    /// Key-Value client for key-value operations.
    kv_client: Arc<Kv>,
    /// Watch client for watch operations.
    watch_client: Watch,
    /// Lease client for lease operations.
    lease_client: Lease,
    /// Lock client for lock operations.
    lock_client: Lock,
}

impl Client {
    /// Get grpc channel.
    fn get_channel(cfg: &ClientConfig) -> Channel {
        assert!(!cfg.endpoints.is_empty(), "Empty etcd endpoints");

        let mut end_points = cfg.endpoints.join(",");
        let env = Arc::new(EnvBuilder::new().build());
        if cfg.endpoints.len() > 1 {
            let socket_address: SocketAddr = cfg
                .endpoints
                .first()
                .unwrap_or_else(|| panic!("Fail to get the first endpoint"))
                .parse()
                .unwrap_or_else(|e| {
                    panic!(
                        "Fail to parse enpoint to socket address, the error is {}",
                        e,
                    )
                });
            cfg.endpoints.iter().for_each(|endpoint| {
                let ip: SocketAddr = endpoint.parse().unwrap_or_else(|e| {
                    panic!(
                        "Fail to parse enpoint to socket address, the error is {}",
                        e,
                    )
                });
                if !(socket_address.is_ipv4() && ip.is_ipv4()
                    || socket_address.is_ipv6() && ip.is_ipv6())
                {
                    panic!("Endpoints have different type of ip address schema");
                }
            });

            if socket_address.is_ipv4() {
                end_points = format!("{}:{}", "ipv4", end_points);
            } else if socket_address.is_ipv6() {
                end_points = format!("{}:{}", "ipv6", end_points);
            } else {
                panic!("unsupported etcd address: {}", socket_address)
            }
        }
        let ch = ChannelBuilder::new(env)
            .load_balancing_policy(LbPolicy::RoundRobin)
            .connect(end_points.as_str());
        ch
    }

    /// Connects to etcd generate auth token.
    /// The client connection used to request the authentication token is typically thrown away;
    /// it cannot carry the new token’s credentials. This is because `gRPC` doesn’t provide a way
    /// for adding per RPC credential after creation of the connection.
    // async fn generate_auth_token(cfg: &ClientConfig) -> Result<Option<String>> {
    //     use crate::AuthenticateRequest;

    //     let channel = Self::get_channel(&cfg)?;

    //     let mut auth_client = Auth::new(AuthClient::new(channel));

    //     let token = match cfg.auth.as_ref() {
    //         Some((name, password)) => auth_client
    //             .authenticate(AuthenticateRequest::new(name, password))
    //             .await
    //             .map(|r| Some(r.token().to_owned()))?,
    //         None => None,
    //     };

    //     Ok(token)
    // }

    /// Connects to etcd cluster and returns a client.
    ///
    /// # Errors
    /// Will returns `Err` if failed to contact with given endpoints or authentication failed.
    #[inline]
    pub async fn connect(cfg: ClientConfig) -> Result<Self> {
        let channel = Self::get_channel(&cfg);
        let etcd_watch_client = WatchClient::new(channel.clone());

        Ok(Self {
            inner: Arc::new(Inner {
                channel: channel.clone(),
                auth_client: Auth::new(AuthClient::new(channel.clone())),
                kv_client: Kv::new(
                    KvClient::new(channel.clone()),
                    etcd_watch_client.clone(),
                    cfg.cache_size,
                    cfg.cache_enable,
                ),
                watch_client: Watch::new(etcd_watch_client),
                lease_client: Lease::new(LeaseClient::new(channel.clone())),
                lock_client: Lock::new(LockClient::new(channel)),
            }),
        })
    }

    /// Gets an auth client.
    #[inline]
    #[must_use]
    pub fn auth(&self) -> Auth {
        self.inner.auth_client.clone()
    }

    /// Gets a key-value client.
    #[inline]
    #[must_use]
    pub fn kv(&self) -> Arc<Kv> {
        Arc::<Kv>::clone(&self.inner.kv_client)
    }

    /// Get a lock client.
    #[inline]
    #[must_use]
    pub fn lock(&self) -> Lock {
        self.inner.lock_client.clone()
    }

    /// Gets a watch client.
    #[inline]
    #[must_use]
    pub fn watch_client(&self) -> Watch {
        self.inner.watch_client.clone()
    }

    /// Perform a watch operation
    #[inline]
    pub async fn watch(
        &self,
        key_range: KeyRange,
    ) -> impl Stream<Item = Result<EtcdWatchResponse>> {
        let mut client = self.inner.watch_client.clone();
        client.watch(key_range).await
    }

    /// Gets a lease client.
    #[inline]
    #[must_use]
    pub fn lease(&self) -> Lease {
        self.inner.lease_client.clone()
    }

    /// Shut down any running tasks.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn shutdown(&self) -> Result<()> {
        let mut watch_client = self.inner.watch_client.clone();
        watch_client.shutdown().await?;
        let mut lease_client = self.inner.lease_client.clone();
        lease_client.shutdown().await?;
        self.inner.kv_client.shutdown().await;
        Ok(())
    }
}
