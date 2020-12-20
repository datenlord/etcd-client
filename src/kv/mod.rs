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
pub use put::{EtcdPutRequest, EtcdPutResponse};
pub use range::{EtcdRangeRequest, EtcdRangeResponse};
pub use txn::{EtcdTxnRequest, EtcdTxnResponse, TxnCmp, TxnOpResponse};

use super::OverflowArithmetic;
use crate::protos::kv::Event_EventType;
use crate::protos::rpc::RangeResponse;
use crate::protos::rpc_grpc::{KvClient, WatchClient};
use futures::stream::StreamExt;
use grpcio::WriteFlags;
use log::warn;
use protobuf::RepeatedField;
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
        let cache = Arc::new(Cache::new(etcd_cache_size));
        let (mut client_req_sender, mut client_resp_receiver) = watch_client
            .watch()
            .unwrap_or_else(|e| panic!("failed to send watch commend, the response is: {}", e));

        let cache_inner = Arc::<Cache>::clone(&cache);
        smol::spawn(async move {
            let watch_request = EtcdWatchRequest::create(KeyRange::all());
            client_req_sender
                .send((watch_request.into(), WriteFlags::default()))
                .await
                .unwrap_or_else(|e| panic!("Fail to send watch request, the error is: {}", e));

            while let Some(watch_resp) = client_resp_receiver.next().await {
                match watch_resp {
                    Ok(resp) => {
                        let events = resp.get_events().to_vec();
                        events.iter().for_each(|event| {
                            if event.get_field_type() == Event_EventType::PUT {
                                let value = cache_inner.search(event.get_kv().get_key());
                                if let Some(valid_value) = value {
                                    if valid_value.version >= event.get_kv().get_version() {
                                        return;
                                    }
                                }
                                cache_inner.insert(
                                    event.get_kv().get_key().to_vec(),
                                    event.get_kv().clone(),
                                );
                            } else {
                                cache_inner.delete(event.get_kv().get_key());
                            }
                        })
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
        }
    }

    /// Performs a key-value saving operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn put(&mut self, req: EtcdPutRequest) -> Res<EtcdPutResponse> {
        let resp = self.client.put_async(&req.into())?;
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
            kvs.iter().for_each(|kv| {
                self.cache.insert(kv.get_key().to_vec(), kv.clone());
            });
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
