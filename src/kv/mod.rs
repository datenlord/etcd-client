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

pub use super::watch::EtcdWatchRequest;
pub use cache::Cache;
pub use delete::{EtcdDeleteRequest, EtcdDeleteResponse};
pub use get::{EtcdGetRequest, EtcdGetResponse};
use grpcio::{ClientDuplexSender, WriteFlags};
pub use put::{EtcdPutRequest, EtcdPutResponse};
pub use range::{EtcdRangeRequest, EtcdRangeResponse};
pub use txn::{EtcdTxnRequest, EtcdTxnResponse, TxnCmp, TxnOpResponse};

use super::OverflowArithmetic;
use crate::protos::kv::Event_EventType;
use crate::protos::rpc::{RangeResponse, WatchRequest};
use crate::protos::rpc_grpc::{KvClient, WatchClient};
use crossbeam_queue::SegQueue;
use futures::stream::StreamExt;
use log::warn;
use protobuf::RepeatedField;
use smol::lock::Mutex;
use std::str;

use crate::protos::kv::KeyValue;
use crate::Result as Res;
use futures::SinkExt;
use std::sync::Arc;
use utilities::Cast;

/// Key-Value client.
#[derive(Clone)]
pub struct Kv {
    /// Etcd Key-Value client.
    client: KvClient,
    /// Etcd client cache.
    cache: Arc<Cache>,
    /// Enable etcd client cache.
    cache_enable: bool,
    /// Etcd watch request sender.
    watch_req_sender: Arc<Mutex<ClientDuplexSender<WatchRequest>>>,
    /// Etcd watch request sending queue.
    watch_req_queue: Arc<SegQueue<WatchRequest>>,
}

/// Etcd client cache default size.
const ETCD_CACHE_DEFAULT_SIZE: usize = 64;

impl Kv {
    /// Creates a new `KvClient`.
    ///
    /// This method should only be called within etcd client.
    pub(crate) fn new(
        client: KvClient,
        watch_client: &WatchClient,
        cache_size: usize,
        cache_enable: bool,
    ) -> Self {
        let etcd_cache_size = if cache_size == 0 {
            ETCD_CACHE_DEFAULT_SIZE
        } else {
            cache_size
        };

        let (client_req_sender, mut client_resp_receiver) = watch_client
            .watch()
            .unwrap_or_else(|e| panic!("failed to send watch commend, the response is: {}", e));

        let cache = Arc::new(Cache::new(etcd_cache_size));
        let watch_req_sender = Arc::new(Mutex::new(client_req_sender));
        let watch_req_queue = Arc::new(SegQueue::<WatchRequest>::new());

        let cache_inner = Arc::<Cache>::clone(&cache);
        let watch_req_sender_inner =
            Arc::<Mutex<ClientDuplexSender<WatchRequest>>>::clone(&watch_req_sender);
        let watch_req_queue_inner = Arc::<SegQueue<WatchRequest>>::clone(&watch_req_queue);

        smol::spawn(async move {
            while let Some(watch_resp) = client_resp_receiver.next().await {
                match watch_resp {
                    Ok(resp) => {
                        if resp.get_created() {
                            if let Some(request) = watch_req_queue_inner.pop() {
                                // Save the watch id to watch_id_table.
                                let key = request.get_create_request().get_key();
                                cache_inner.insert_watch_id(key.to_vec(), resp.get_watch_id());
                            }
                        } else {
                            let events = resp.get_events().to_vec();
                            for i in 0..events.len() {
                                let event = events.get(i).unwrap_or_else(|| {
                                    panic!("Fail to get event from watch reqponse")
                                });
                                let key = event.get_kv().get_key();
                                if event.get_field_type() == Event_EventType::PUT {
                                    let value = cache_inner.search(key);
                                    if let Some(valid_value) = value {
                                        if valid_value.version >= event.get_kv().get_version() {
                                            return;
                                        }
                                    }
                                    cache_inner.insert(key.to_vec(), event.get_kv().clone());
                                } else {
                                    cache_inner.delete(key);
                                    let watch_id = cache_inner.search_watch_id(key);
                                    if let Some(watch_id) = watch_id {
                                        let mut req_sender = watch_req_sender_inner.lock().await;
                                        let watch_request = EtcdWatchRequest::cancel(watch_id);
                                        req_sender
                                            .send((watch_request.into(), WriteFlags::default()))
                                            .await
                                            .unwrap_or_else(|e| {
                                                panic!(
                                                    "Fail to send watch request, the error is: {}",
                                                    e
                                                )
                                            });
                                    } else {
                                        warn!("No watch id found in etcd cache");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Watch response contains error, the error is: {}", e);
                    }
                }
            }
        })
        .detach();
        Self {
            client,
            cache,
            cache_enable,
            watch_req_sender,
            watch_req_queue,
        }
    }

    /// Performs a key-value saving operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn put(&mut self, req: EtcdPutRequest) -> Res<EtcdPutResponse> {
        let key = req.get_key();
        let resp = self.client.put_async(&req.into())?;
        if self.cache.search(&key) == None {
            self.send_watch_request(key).await;
        }
        Ok(From::from(resp.await?))
    }

    /// Performs a single key-value fetching operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn get(&mut self, req: EtcdGetRequest) -> Res<EtcdGetResponse> {
        if self.cache_enable {
            if let Some(value) = self.cache.search(&req.get_key()) {
                let mut response = RangeResponse::new();
                response.set_count(1);
                response.set_kvs(RepeatedField::from_vec(vec![value]));
                return Ok(EtcdGetResponse::new(response));
            }
        }

        let resp = self.client.range_async(&req.into())?.await?;
        if self.cache_enable {
            let kvs = resp.get_kvs();
            for i in 0..kvs.len() {
                let kv = kvs
                    .get(i)
                    .unwrap_or_else(|| panic!("Fail to get kv from GetResponse"));
                if self.cache.search(kv.get_key()) == None {
                    self.send_watch_request(kv.get_key().to_vec()).await;
                }
                self.cache.insert(kv.get_key().to_vec(), kv.clone());
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
    pub async fn range(&mut self, req: EtcdRangeRequest) -> Res<EtcdRangeResponse> {
        let resp = self.client.range_async(&req.into())?.await?;
        if self.cache_enable {
            let kvs = resp.get_kvs();
            kvs.iter().for_each(|kv| {
                self.cache.insert(kv.get_key().to_vec(), kv.clone());
            });
        }
        Ok(From::from(resp))
    }

    /// Performs a key-value deleting operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn delete(&mut self, req: EtcdDeleteRequest) -> Res<EtcdDeleteResponse> {
        let resp = self.client.delete_range_async(&req.into())?;

        Ok(From::from(resp.await?))
    }

    /// Performs a transaction operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn txn(&mut self, req: EtcdTxnRequest) -> Res<EtcdTxnResponse> {
        let resp = self.client.txn_async(&req.into())?;

        Ok(From::from(resp.await?))
    }

    /// Sends watch request for a specific key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    async fn send_watch_request(&self, key: Vec<u8>) {
        let mut req_sender = self.watch_req_sender.lock().await;
        let etcd_watch_request = EtcdWatchRequest::create(KeyRange::key(key));
        let eatch_request: WatchRequest = etcd_watch_request.into();
        req_sender
            .send((eatch_request.clone(), WriteFlags::default()))
            .await
            .unwrap_or_else(|e| panic!("Fail to send watch request, the error is: {}", e));
        self.watch_req_queue.push(eatch_request);
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
