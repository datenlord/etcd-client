mod cache;
/// Etcd delete mod for delete operations.
mod delete;
/// Etcd get mod for get operations.
mod get;
/// Etcd put mod for put operations.
mod put;
/// Etcd range mod for range fetching operations.
mod range;
/// Etcd txn mod for transaction operations.
mod txn;

pub use super::watch::{EtcdWatchRequest, EtcdWatchResponse};
use async_std::channel::Receiver;
pub use cache::Cache;
pub use delete::{EtcdDeleteRequest, EtcdDeleteResponse};
pub use get::{EtcdGetRequest, EtcdGetResponse};
pub use put::{EtcdPutRequest, EtcdPutResponse};
pub use range::{EtcdRangeRequest, EtcdRangeResponse};
pub use txn::{EtcdTxnRequest, EtcdTxnResponse, TxnCmp, TxnOpResponse};

use super::OverflowArithmetic;
use crate::protos::kv::Event_EventType;
use crate::protos::rpc::RangeResponse;
use crate::protos::rpc_grpc::{KvClient, WatchClient};
use crate::retryable;
use crate::CURRENT_INTERVAL_ENV_KEY;
use crate::CURRENT_INTERVAL_VALUE;
use crate::INITIAL_INTERVAL_ENV_KEY;
use crate::INITIAL_INTERVAL_VALUE;
use crate::MAX_ELAPSED_TIME_ENV_KEY;
use crate::MAX_ELAPSED_TIME_VALUE;
use atomic_refcell::AtomicRefCell;
use backoff::ExponentialBackoff;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use grpcio::WriteFlags;
use log::warn;
use protobuf::RepeatedField;
use smol::channel::{bounded, unbounded, Sender};
use smol::lock::RwLock;
use std::str;
use std::sync::Arc;
use std::time::Duration;

use crate::protos::kv::KeyValue;
use crate::Result as Res;
use futures::SinkExt;
use utilities::Cast;

/// Key-Value client.
#[derive(Clone)]
pub struct Kv {
    /// Inner data
    inner: Arc<KvInner>,
}

/// Inner data of Kv
pub struct KvInner {
    /// Etcd Key-Value client.
    client: KvClient,
    /// Etcd client cache.
    cache: Arc<Cache>,
    /// Enable etcd client cache.
    cache_enable: bool,
    /// Etcd watch request sender.
    watch_sender: AtomicRefCell<Sender<EtcdWatchRequest>>,
    /// Read write lock to access cache
    /// When the cache has error, write lock is acquired to disable the cache and clean the cache.
    /// Other operations will get read lock.
    cache_lock: RwLock<()>,
    /// A channel sender to send shutdown request.
    shutdown_task1: AtomicRefCell<Sender<()>>,
    /// A channel sender to send shutdown request.
    shutdown_task2: AtomicRefCell<Sender<()>>,
    /// Watch Client
    watch_client: WatchClient,
}

/// Etcd client cache default size.
const ETCD_CACHE_DEFAULT_SIZE: usize = 64;
impl KvInner {
    /// Creates a new `KvClient`.
    ///
    /// This method should only be called within etcd client.
    pub fn new(
        client: KvClient,
        watch_client: WatchClient,
        cache_size: usize,
        cache_enable: bool,
    ) -> Arc<Self> {
        let etcd_cache_size = if cache_size == 0 {
            ETCD_CACHE_DEFAULT_SIZE
        } else {
            cache_size
        };

        let cache = Arc::new(Cache::new(etcd_cache_size));
        let (watch_req_sender, watch_req_receiver) = unbounded::<EtcdWatchRequest>();
        let (shutdown_tx1, shutdown_rx_1) = unbounded();
        let (shutdown_tx2, shutdown_rx_2) = unbounded();

        let cache_clone = Arc::<Cache>::clone(&cache);
        let watch_client_clone = watch_client.clone();
        let this = Arc::new(Self {
            client,
            cache,
            cache_enable,
            watch_sender: AtomicRefCell::new(watch_req_sender),
            cache_lock: RwLock::new(()),
            shutdown_task1: AtomicRefCell::new(shutdown_tx1),
            shutdown_task2: AtomicRefCell::new(shutdown_tx2),
            watch_client,
        });

        if cache_enable {
            Self::start_watch_task(
                Arc::<Self>::clone(&this),
                cache_clone,
                shutdown_rx_1,
                shutdown_rx_2,
                watch_req_receiver,
                &watch_client_clone,
            )
        };
        this
    }

    /// Start async watch task
    #[allow(clippy::mut_mut)]
    #[allow(clippy::too_many_lines)]
    fn start_watch_task(
        this_clone: Arc<Self>,
        cache_clone: Arc<Cache>,
        shutdown_rx_1: Receiver<()>,
        shutdown_rx_2: Receiver<()>,
        watch_req_receiver: Receiver<EtcdWatchRequest>,
        watch_client: &WatchClient,
    ) {
        let (mut client_req_sender, mut client_resp_receiver) = watch_client
            .watch()
            .unwrap_or_else(|e| panic!("failed to send watch commend, the response is: {}", e));
        let (watch_id_sender, watch_id_receiver) = bounded::<i64>(1);
        let this_clone2 = Arc::<Self>::clone(&this_clone);
        let cache_clone2 = Arc::<Cache>::clone(&cache_clone);
        // Task that handles all the pending watch requests.
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx_1.into_future().fuse();
            loop {
                let watch_req = futures::select! {
                    req_opt = watch_req_receiver.recv().fuse() => {
                        if let Ok(req) = req_opt {
                            req
                        } else {
                            warn!("Failed to receive watch request");
                            Self::restart_cache(Arc::<Self>::clone(&this_clone)).await;
                            return;
                        }

                    }
                    _ = shutdown_rx => return

                };
                let processing_key = watch_req.get_key();
                let res = client_req_sender
                    .send((watch_req.clone().into(), WriteFlags::default()))
                    .await;

                if let Err(e) = res {
                    warn!(
                        "Fail to send watch request, the error is: {}, clean cache",
                        e
                    );
                    Self::restart_cache(Arc::<Self>::clone(&this_clone)).await;
                    return;
                }
                // Wait until etcd server returns watch id.
                let watch_id = match watch_id_receiver.recv().await {
                    Err(e) => {
                        warn!(
                            "Fail to receive watch id from channel, the error is {}, clean cache",
                            e
                        );
                        Self::restart_cache(Arc::<Self>::clone(&this_clone)).await;
                        return;
                    }
                    Ok(id) => id,
                };
                // Watch request can only be create or cancel.
                if watch_req.is_create() {
                    this_clone.cache_lock.read().await;
                    cache_clone
                        .update_watch_id(processing_key.clone(), watch_id)
                        .await;
                } else {
                    this_clone.cache_lock.write().await;
                    cache_clone.remove(&processing_key).await;
                }
            }
        })
        .detach();

        // Task that handle the watch responses from Etcd server.
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx_2.into_future().fuse();
            loop {
                let watch_resp = futures::select! {
                    resp_opt = client_resp_receiver.next().fuse() => {
                        if let Some(resp) = resp_opt {
                            resp
                        } else {
                            warn!("failed to receive watch response from etcd");
                            Self::restart_cache(Arc::<Self>::clone(&this_clone2)).await;
                            return;
                        }
                    },
                    _ = shutdown_rx => return
                };

                match watch_resp {
                    Ok(resp) => {
                        // TODO: Check if need to spawn new task here.
                        if resp.get_created() || resp.get_canceled() {
                            if let Err(e) = watch_id_sender.send(resp.get_watch_id()).await {
                                warn!("Fail to send watch id, the error is {}, clean cache", e);
                                Self::restart_cache(Arc::<Self>::clone(&this_clone2)).await;
                                return;
                            }
                        } else {
                            let events = resp.get_events().to_vec();
                            this_clone2.cache_lock.read().await;
                            for event in events {
                                if event.get_field_type() == Event_EventType::PUT {
                                    cache_clone2
                                        .update(
                                            event.get_kv().get_key().to_vec(),
                                            event.get_kv().clone(),
                                            false,
                                        )
                                        .await;
                                } else {
                                    cache_clone2
                                        .mark_delete(
                                            event.get_kv().get_key().to_vec(),
                                            event.get_kv().clone(),
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Watch response contains error, the error is: {}, clean cache",
                            e
                        );
                        Self::restart_cache(Arc::<Self>::clone(&this_clone2)).await;
                        return;
                    }
                }
            }
        })
        .detach();
    }
    /// Clean cache
    #[inline]
    async fn restart_cache(self: Arc<Self>) {
        self.cache_lock.write().await;
        self.cache.clean().await;
        let mut shutdown1 = self.shutdown_task1.borrow_mut();
        let mut shutdown2 = self.shutdown_task2.borrow_mut();
        let (watch_req_sender, watch_req_receiver) = unbounded::<EtcdWatchRequest>();
        let (shutdown_tx1, shutdown_rx_1) = unbounded();
        let (shutdown_tx2, shutdown_rx_2) = unbounded();
        let mut watch_sender = self.watch_sender.borrow_mut();
        shutdown1
            .send(())
            .await
            .unwrap_or_else(|e| panic!("failed to shutdown error is {:?}", e));
        shutdown2
            .send(())
            .await
            .unwrap_or_else(|e| panic!("failed to shutdown error is {:?}", e));
        shutdown1.close();
        shutdown2.close();
        Self::start_watch_task(
            Arc::<Self>::clone(&self),
            Arc::<Cache>::clone(&self.cache),
            shutdown_rx_1,
            shutdown_rx_2,
            watch_req_receiver,
            &self.watch_client,
        );
        *shutdown1 = shutdown_tx1;
        *shutdown2 = shutdown_tx2;
        *watch_sender = watch_req_sender;
    }
}
impl Kv {
    /// Creates a new `KvClient`.
    ///
    /// This method should only be called within etcd client.
    pub(crate) fn new(
        client: KvClient,
        watch_client: WatchClient,
        cache_size: usize,
        cache_enable: bool,
    ) -> Self {
        Self {
            inner: KvInner::new(client, watch_client, cache_size, cache_enable),
        }
    }
    /// Performs a key-value saving operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn put(&self, req: EtcdPutRequest) -> Res<EtcdPutResponse> {
        let resp: EtcdPutResponse = retryable!(|| async {
            let resp = self.inner.client.put_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Performs a single key-value fetching operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn get(&self, req: EtcdGetRequest) -> Res<EtcdGetResponse> {
        if self.inner.cache_enable && self.inner.cache_lock.try_read().is_some() {
            if let Some(value) = self.inner.cache.search(req.get_key()).await {
                let mut response = RangeResponse::new();
                response.set_count(1);
                response.set_kvs(RepeatedField::from_vec(vec![value]));
                return Ok(EtcdGetResponse::new(response));
            }
        }

        let resp = retryable!(|| async {
            let resp = self.inner.client.range_async(&req.clone().into())?;
            Ok(resp.await?)
        });
        if self.inner.cache_enable {
            if let Some(cache_lock) = self.inner.cache_lock.try_write() {
                let kvs = resp.get_kvs();
                for kv in kvs {
                    if self
                        .inner
                        .cache
                        .search(kv.get_key().to_vec())
                        .await
                        .is_none()
                    {
                        self.inner
                            .cache
                            .insert_or_update(kv.get_key().to_vec(), kv.clone())
                            .await;
                        // Creates a new watch request and adds to the send queue.
                        let mut watch_request =
                            EtcdWatchRequest::create(KeyRange::key(kv.get_key()));
                        watch_request.set_start_revision(kv.get_mod_revision().cast());
                        if let Err(e) = self.inner.watch_sender.borrow().send(watch_request).await {
                            warn!(
                                "Fail to send watch request, the error is {}, clean cache",
                                e
                            );
                            drop(cache_lock);
                            KvInner::restart_cache(Arc::<KvInner>::clone(&self.inner)).await;
                            break;
                        }
                    } else {
                        self.inner
                            .cache
                            .update(kv.get_key().to_vec(), kv.clone(), false)
                            .await;
                    }
                    if let Err(e) = self
                        .inner
                        .cache
                        .adjust_cache_size(&self.inner.watch_sender.borrow())
                        .await
                    {
                        warn!(
                            "Fail to send watch request, the error is {}, clean cache",
                            e
                        );
                        drop(cache_lock);
                        KvInner::restart_cache(Arc::<KvInner>::clone(&self.inner)).await;
                        break;
                    }
                }
            }
        }
        Ok(From::from(resp))
    }

    /// Performs a range key-value fetching operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn range(&self, req: EtcdRangeRequest) -> Res<EtcdRangeResponse> {
        let resp = retryable!(|| async {
            let resp = self.inner.client.range_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Performs a key-value deleting operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn delete(&self, req: EtcdDeleteRequest) -> Res<EtcdDeleteResponse> {
        let resp = retryable!(|| async {
            let resp = self.inner.client.delete_range_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Performs a transaction operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn txn(&self, req: EtcdTxnRequest) -> Res<EtcdTxnResponse> {
        let resp = retryable!(|| async {
            let resp = self.inner.client.txn_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Shut down the running watch task, if any.
    ///
    /// # Errors
    ///
    /// Will return `Err` if kv is shutdown.
    #[inline]
    pub async fn shutdown(&self) -> Res<()> {
        if self.inner.cache_enable {
            self.inner.cache_lock.write().await;
            self.inner.shutdown_task1.borrow().send(()).await?;
            self.inner.shutdown_task2.borrow().send(()).await?;
            self.inner.shutdown_task1.borrow().close();
            self.inner.shutdown_task2.borrow().close();
        }
        Ok(())
    }
}

/// Key-Value pair.
#[derive(Clone, PartialEq)]
pub struct EtcdKeyValue {
    /// Etcd `KeyValue` pairs struct.
    proto: KeyValue,
}

impl EtcdKeyValue {
    /// Gets the key in bytes. An empty key is not allowed.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.proto.key
    }

    /// Takes the key out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::replace(&mut self.proto.key, vec![])
    }

    /// Converts the key from bytes `&[u8]` to `&str`.
    /// Leaves the original `&[u8]` in place, and creates a new string slice containing the entire content.
    #[inline]
    pub fn key_str(&self) -> &str {
        std::str::from_utf8(&self.proto.key)
            .unwrap_or_else(|e| panic!("Fail to convert bytes to string, the error is: {}", e))
    }

    /// Gets the value held by the key, in bytes.
    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.proto.value
    }

    /// Takes the value out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_value(&mut self) -> Vec<u8> {
        std::mem::replace(&mut self.proto.value, vec![])
    }

    /// Converts the value from bytes `&[u8]` to `&str`.
    /// Leaves the original `&[u8]` in place, and creates a new string slice containing the entire content.
    #[inline]
    pub fn value_str(&self) -> &str {
        std::str::from_utf8(&self.proto.value)
            .unwrap_or_else(|e| panic!("Fail to convert bytes to string, the error is {}", e))
    }

    /// Gets the revision of last creation on this key.
    #[inline]
    pub fn create_revision(&self) -> usize {
        self.proto.create_revision.cast()
    }

    /// Gets the revision of last modification on this key.
    #[inline]
    pub fn mod_revision(&self) -> usize {
        self.proto.mod_revision.cast()
    }

    /// Gets the version of the key.
    #[inline]
    pub fn version(&self) -> usize {
        self.proto.version.cast()
    }

    /// Gets the ID of the lease that attached to key.
    #[inline]
    pub fn lease(&self) -> usize {
        self.proto.lease.cast()
    }

    /// Returns `true` if this `KeyValue` has a lease attached, and `false` otherwise.
    #[inline]
    pub const fn has_lease(&self) -> bool {
        self.proto.lease != 0
    }
}

impl From<KeyValue> for EtcdKeyValue {
    #[inline]
    fn from(kv: KeyValue) -> Self {
        Self { proto: kv }
    }
}

/// `KeyRange` is an abstraction for describing etcd key of various types.
pub struct KeyRange {
    /// The first key of the range and should be non-empty
    key: Vec<u8>,
    /// The key following the last key of the range
    range_end: Vec<u8>,
}

impl KeyRange {
    /// Creates a new `KeyRange` for describing a range of multiple keys.
    #[inline]
    pub fn range<K, R>(key: K, range_end: R) -> Self
    where
        K: Into<Vec<u8>>,
        R: Into<Vec<u8>>,
    {
        Self {
            key: key.into(),
            range_end: range_end.into(),
        }
    }

    /// Creates a new `KeyRange` for describing a specified key.
    #[inline]
    pub fn key<K>(key: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        Self {
            key: key.into(),
            range_end: vec![],
        }
    }

    /// Creates a new `KeyRange` for describing all keys.
    #[inline]
    #[must_use]
    pub fn all() -> Self {
        Self {
            key: vec![0],
            range_end: vec![0],
        }
    }

    /// Creates a new `KeyRange` for describing keys prefixed with specified value.
    #[inline]
    pub fn prefix<K>(prefix: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        let key = prefix.into();
        if key.is_empty() {
            // An empty Vec<u8> results in an invalid KeyRange.
            // Assume that an empty value passed to this method implies no prefix (i.e., all keys).
            return Self::all();
        }

        let mut first_value = true;
        let mut range_end = key
            .iter()
            .rev()
            .filter_map(|e| {
                if *e < 0xff {
                    if first_value {
                        first_value = false;
                        Some(e.overflow_add(1))
                    } else {
                        Some(*e)
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<u8>>();
        range_end.reverse();
        Self { key, range_end }
    }

    /// Take key value
    #[inline]
    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::replace(&mut self.key, vec![])
    }

    /// Take `range_end` value
    #[inline]
    pub fn take_range_end(&mut self) -> Vec<u8> {
        std::mem::replace(&mut self.range_end, vec![])
    }
}
