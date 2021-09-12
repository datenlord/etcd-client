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
use backoff::ExponentialBackoff;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use grpcio::WriteFlags;
use log::warn;
use protobuf::RepeatedField;
use smol::channel::{bounded, unbounded, Sender};
use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::protos::kv::KeyValue;
use crate::Result as Res;
use arc_swap::ArcSwap;
use clippy_utilities::Cast;
use futures::SinkExt;

/// Key-Value client.
pub struct Kv {
    /// Etcd Key-Value client.
    client: KvClient,
    /// Watch Client
    watch_client: WatchClient,
    /// Kv Cache Size
    cache_size: usize,
    /// Kv Cache if etcd client cache is enabled otherwise None
    kvcache: Option<ArcSwap<KvCache>>,
    /// Lock to restart cache
    restart_lock: Mutex<()>,
}

/// Cache of Kv
pub struct KvCache {
    /// Etcd client cache.
    cache: Arc<Cache>,
    /// Etcd watch request sender.
    watch_sender: Sender<WatchRequest>,
    /// A channel sender to send shutdown request.
    shutdown_task1: Sender<()>,
    /// A channel sender to send shutdown request.
    shutdown_task2: Sender<()>,
    /// Arc to `Kv` that contains this `KvCache`
    kv: Weak<Kv>,
}

/// Etcd client cache default size.
const ETCD_CACHE_DEFAULT_SIZE: usize = 256;
impl KvCache {
    /// Creates a new `KvClient`.
    ///
    /// This method should only be called within etcd client.
    pub fn new(watch_client: &WatchClient, cache_size: usize, kv: Weak<Kv>) -> Arc<Self> {
        let etcd_cache_size = if cache_size == 0 {
            ETCD_CACHE_DEFAULT_SIZE
        } else {
            cache_size
        };

        let cache = Arc::new(Cache::new(etcd_cache_size));

        let cache_clone = Arc::<Cache>::clone(&cache);
        let (watch_req_sender, watch_req_receiver) = unbounded::<WatchRequest>();
        let (shutdown_tx1, shutdown_rx_1) = unbounded();
        let (shutdown_tx2, shutdown_rx_2) = unbounded();
        let this = Arc::new(Self {
            cache,
            watch_sender: watch_req_sender,
            shutdown_task1: shutdown_tx1,
            shutdown_task2: shutdown_tx2,
            kv,
        });

        Self::start_watch_task(
            Arc::<Self>::clone(&this),
            cache_clone,
            shutdown_rx_1,
            shutdown_rx_2,
            watch_req_receiver,
            watch_client,
        );

        this
    }

    /// Restart cache
    fn restart_cache(&self) {
        if let Some(kv) = self.kv.upgrade() {
            kv.restart_kvcache();
        }
    }

    /// Start async watch task
    #[allow(clippy::mut_mut)]
    #[allow(clippy::too_many_lines)]
    fn start_watch_task(
        this_clone: Arc<Self>,
        cache_clone: Arc<Cache>,
        shutdown_rx_1: Receiver<()>,
        shutdown_rx_2: Receiver<()>,
        watch_req_receiver: Receiver<WatchRequest>,
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
            let mut watch_map = HashMap::<Vec<u8>, i64>::new();
            let mut shutdown_rx = shutdown_rx_1.into_future().fuse();

            loop {
                let watch_req = futures::select! {
                    req_opt = watch_req_receiver.recv().fuse() => {
                        if let Ok(req) = req_opt {
                            req
                        } else {
                            warn!("Failed to receive watch request");
                            this_clone.restart_cache();
                            return;
                        }

                    }
                    _ = shutdown_rx => return

                };
                let processing_key = watch_req.key;
                // If key is already watched, skip create watch request
                // If cancel watch request has been sent(watch id == -1), skip cancel watch request
                // If key is not watched, skip cancel watch request
                if (watch_req.is_create && watch_map.contains_key(&processing_key))
                    || (!watch_req.is_create && !watch_map.contains_key(&processing_key))
                {
                    continue;
                }

                let request = if watch_req.is_create {
                    let mut req = EtcdWatchRequest::create(KeyRange::key(processing_key.clone()));
                    req.set_start_revision(watch_req.revision.cast());
                    req
                } else {
                    let req = EtcdWatchRequest::cancel(
                        (*watch_map
                            .get(&processing_key)
                            .unwrap_or_else(|| panic!("key {:?} doesn't exist", processing_key)))
                        .cast(),
                    );
                    req
                };

                let res = client_req_sender
                    .send((request.into(), WriteFlags::default()))
                    .await;

                if let Err(e) = res {
                    warn!(
                        "Fail to send watch request, the error is: {}, clean cache",
                        e
                    );
                    this_clone.restart_cache();
                    return;
                }
                // Wait until etcd server returns watch id.
                let watch_id = match watch_id_receiver.recv().await {
                    Err(e) => {
                        warn!(
                            "Fail to receive watch id from channel, the error is {}, clean cache",
                            e
                        );
                        this_clone.restart_cache();
                        return;
                    }
                    Ok(id) => id,
                };
                // Watch request can only be create or cancel.
                if watch_req.is_create {
                    watch_map.insert(processing_key, watch_id);
                } else {
                    watch_map.remove(&processing_key);
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
                            this_clone2.restart_cache();
                            return;
                        }
                    },
                    _ = shutdown_rx => return
                };

                match watch_resp {
                    Ok(resp) => {
                        if resp.get_created() || resp.get_canceled() {
                            if let Err(e) = watch_id_sender.send(resp.get_watch_id()).await {
                                warn!("Fail to send watch id, the error is {}, clean cache", e);
                                this_clone2.restart_cache();
                                return;
                            }
                        } else {
                            let events = resp.get_events().to_vec();
                            for event in events {
                                if event.get_field_type() == Event_EventType::PUT {
                                    cache_clone2
                                        .insert_or_update(
                                            event.get_kv().get_key().to_vec(),
                                            event.get_kv().clone(),
                                        ).await;
                                } else {
                                    cache_clone2
                                        .mark_delete(
                                            event.get_kv().get_key().to_vec(),
                                            event.get_kv().clone(),
                                        ).await;
                                    {
                                        let res = this_clone2
                                            .watch_sender
                                            .send(WatchRequest::cancel(
                                                event.get_kv().get_key().to_vec(),
                                            ))
                                            .await;
                                        if let Err(e) = res {
                                            warn!(
                                                "Fail to send watch request, the error is: {}, clean cache",
                                                e
                                            );
                                            this_clone2.restart_cache();
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Watch response contains error, the error is: {}, clean cache",
                            e
                        );
                        this_clone2.restart_cache();
                        return;
                    }
                }
            }
        })
        .detach();
    }
    /// Clean cache
    #[inline]
    async fn shutdown_cache(&self) -> Res<()> {
        self.shutdown_task1.send(()).await?;
        self.shutdown_task1.send(()).await?;
        self.shutdown_task1.close();
        self.shutdown_task2.close();
        Ok(())
    }
}
impl Kv {
    /// Creates a new `KvClient`.
    ///
    /// This method should only be called within etcd client.
    #[allow(unsafe_code)]
    #[allow(clippy::as_conversions)]
    pub(crate) fn new(
        client: KvClient,
        watch_client: WatchClient,
        cache_size: usize,
        cache_enable: bool,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            client,
            watch_client,
            cache_size,
            kvcache: None,
            restart_lock: Mutex::<()>::new(()),
        });

        if cache_enable {
            let kvcache = Some(ArcSwap::from(KvCache::new(
                &this.watch_client,
                cache_size,
                Arc::<Self>::downgrade(&this),
            )));
            // SAFETY: it is safe because this is constructor.
            // TODO: change to Arc::new_cyclic when 1.60 is released.
            unsafe {
                (*(Arc::<Self>::as_ptr(&this) as *mut Self)).kvcache = kvcache;
            }
        }
        this
    }

    /// Restart `KvCache`
    fn restart_kvcache(&self) {
        if self.restart_lock.try_lock().is_ok() {
            if let Some(ref kvcache) = self.kvcache {
                let self_weak = Weak::<Self>::clone(&kvcache.load().kv);
                let new_kvcache = KvCache::new(&self.watch_client, self.cache_size, self_weak);
                kvcache.store(new_kvcache);
            }
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
            let resp = self.client.put_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        // Wait until cache is updated and then return
        if let Some(ref kvcache) = self.kvcache {
            while let Some(kv) = kvcache.load().cache.search(req.get_key()).await {
                if kv.get_mod_revision() >= resp.get_revision() {
                    break;
                }
            }
        }
        Ok(resp)
    }

    /// Performs a single key-value fetching operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn get(&self, req: EtcdGetRequest) -> Res<EtcdGetResponse> {
        // cache is enabled
        if let Some(ref kvcache) = self.kvcache {
            if let Some(value) = kvcache.load().cache.search(req.get_key()).await {
                let mut response = RangeResponse::new();
                response.set_count(1);
                response.set_kvs(RepeatedField::from_vec(vec![value]));
                return Ok(EtcdGetResponse::new(response));
            }
        }

        let resp = retryable!(|| async {
            let resp = self.client.range_async(&req.clone().into())?;
            Ok(resp.await?)
        });

        if let Some(ref kvcache_arc) = self.kvcache {
            let kvs = resp.get_kvs();
            let kvcache = kvcache_arc.load();
            for kv in kvs {
                let (succeed, is_insert) = kvcache
                    .cache
                    .insert_or_update(kv.get_key().to_vec(), kv.clone())
                    .await;
                if succeed && is_insert {
                    // Creates a new watch request and adds to the send queue.
                    let watch_request =
                        WatchRequest::create(kv.get_key().to_vec(), kv.get_mod_revision());
                    if let Err(e) = kvcache.watch_sender.send(watch_request).await {
                        warn!(
                            "Fail to send watch request, the error is {}, clean cache",
                            e
                        );
                        self.restart_kvcache();
                        return Err(e.into());
                    }

                    // Adjust cache size
                    if let Err(e) = kvcache.cache.adjust_cache_size(&kvcache.watch_sender).await {
                        warn!(
                            "Fail to send watch request, the error is {}, clean cache",
                            e
                        );
                        self.restart_kvcache();
                        return Err(e);
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
        // If the request is single key, use get() instead
        if req.is_single_key() {
            let resp = self
                .get(EtcdGetRequest::new(req.get_key_range().take_key()))
                .await?;
            return Ok(resp.get_inner().into());
        }
        let resp = retryable!(|| async {
            let resp = self.client.range_async(&req.clone().into())?;
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
    pub async fn delete(&self, mut req: EtcdDeleteRequest) -> Res<EtcdDeleteResponse> {
        let request_prev_kv = req.request_prev_kv();
        if self.kvcache.is_some() {
            req.set_prev_kv(true);
        };
        let mut resp: EtcdDeleteResponse = retryable!(|| async {
            let resp = self.client.delete_range_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        // Wait until cache is updated and then return
        if let Some(ref kvcache) = self.kvcache {
            let prev_kv = if request_prev_kv {
                resp.get_prev_kvs()
            } else {
                resp.take_prev_kvs()
            };
            for kv in prev_kv {
                while let Some(kv) = kvcache.load().cache.search(kv.key()).await {
                    if kv.get_mod_revision() >= resp.get_revision() {
                        break;
                    }
                }
            }
        }
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
            let resp = self.client.txn_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(resp)
    }

    /// Shut down the running watch task, if any.
    /// This should only be called when there are
    /// no other threads accessing the cache.
    ///
    /// # Errors
    ///
    /// Will return `Err` if kv is shutdown.
    #[inline]
    pub async fn shutdown(&self) -> Res<()> {
        if let Some(ref kvcache) = self.kvcache {
            kvcache.load().shutdown_cache().await
        } else {
            Ok(())
        }
    }
}

/// Watch request struct
#[derive(Debug, Clone)]
pub struct WatchRequest {
    /// Request key
    key: Vec<u8>,
    /// Revision to start watch
    /// Set to -1 if the request is to cancel
    revision: i64,
    /// Create watch request or cancel watch request
    is_create: bool,
}

impl WatchRequest {
    /// Create watch request
    fn create(key: Vec<u8>, revision: i64) -> Self {
        Self {
            key,
            revision,
            is_create: true,
        }
    }

    /// Create watch request
    fn cancel(key: Vec<u8>) -> Self {
        Self {
            key,
            revision: -1,
            is_create: false,
        }
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
        std::mem::take(&mut self.proto.key)
    }

    /// Converts the key from bytes `&[u8]` to `&str`.
    /// Leaves the original `&[u8]` in place, and creates a new string slice containing the entire content.
    ///
    /// # Panics
    ///
    /// Will panic if convert fail.
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
        std::mem::take(&mut self.proto.value)
    }

    /// Converts the value from bytes `&[u8]` to `&str`.
    /// Leaves the original `&[u8]` in place, and creates a new string slice containing the entire content.
    ///
    /// # Panics
    ///
    /// Will panic if convert fail.
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
        std::mem::take(&mut self.key)
    }

    /// Take `range_end` value
    #[inline]
    pub fn take_range_end(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.range_end)
    }
}
