//! The implementation for Mock Etcd

use super::kv::{Event, Event_EventType, KeyValue};
use super::lock::{LockRequest, LockResponse, UnlockRequest, UnlockResponse};
use super::lock_grpc::{create_lock, Lock};
use super::rpc::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse, WatchRequest,
    WatchResponse,
};
use super::rpc_grpc::{create_kv, create_lease, create_watch, Kv, Lease, Watch};
use crate::rpc::{RequestOp, ResponseOp};
use async_io::Timer;
use async_lock::MutexGuard;
use async_recursion::async_recursion;
use futures::future::TryFutureExt;
use futures::prelude::*;
use grpcio::{
    DuplexSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
    ServerBuilder, UnarySink, WriteFlags,
};
use log::{debug, error, warn};
use protobuf::RepeatedField;
use smol::lock::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use utilities::{Cast, OverflowArithmetic};

/// Help function to send success `gRPC` response
async fn success<R: Send>(response: R, sink: UnarySink<R>, error_context: &str) {
    sink.success(response)
        .map_err(|e| {
            error!(
                "failed to send response, the error is: {:?}, error_context:{}",
                e, error_context
            );
        })
        .map(|_| ())
        .await;
}

/// Send failure `gRPC` response
fn fail<R>(ctx: &RpcContext, sink: UnarySink<R>, rsc: RpcStatusCode, details: String) {
    debug_assert_ne!(
        rsc,
        RpcStatusCode::OK,
        "the input RpcStatusCode should not be OK"
    );
    let rs = RpcStatus::with_message(rsc, details.clone());
    let f = sink
        .fail(rs)
        .map_err(move |e| {
            error!(
                "failed to send response, the error is: {:?}, details:{}",
                e, details
            );
        })
        .map(|_| ());
    ctx.spawn(f);
}

impl Default for MockEtcdServer {
    #[must_use]
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Mock Etcd Server
#[derive(Debug)]
pub struct MockEtcdServer {
    /// grpc server
    server: Server,
}

impl MockEtcdServer {
    /// Create `MockEtcdServer`
    ///
    /// # Panics
    ///
    /// Will panic if build etcd server fail.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let mock_etcd = MockEtcd::new();
        let etcd_service = create_kv(mock_etcd.clone());
        let etcd_watch_service = create_watch(mock_etcd.clone());
        let etcd_lock_service = create_lock(mock_etcd.clone());
        let etcd_lease_service = create_lease(mock_etcd);
        Self {
            server: ServerBuilder::new(Arc::new(Environment::new(1)))
                .register_service(etcd_service)
                .register_service(etcd_watch_service)
                .register_service(etcd_lock_service)
                .register_service(etcd_lease_service)
                .bind("127.0.0.1", 2379)
                .build()
                .unwrap_or_else(|e| panic!("failed to build etcd server, the error is: {:?}", e)),
        }
    }

    /// Start Mock Etcd Server
    #[inline]
    pub fn start(&mut self) {
        self.server.start();
    }
}

/// `KeyRange` is an abstraction for describing etcd key of various types.
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct KeyRange {
    /// The first key of the range and should be non-empty
    key: Vec<u8>,
    /// The key following the last key of the range
    range_end: Vec<u8>,
}

/// Mock Etcd
#[derive(Clone)]
struct MockEtcd {
    /// Inner Data
    inner: Arc<Mutex<MockEtcdInner>>,
}

/// Mocd Etcd Inner
struct MockEtcdInner {
    /// map to store key value
    map: HashMap<Vec<u8>, KeyValue>,
    /// map to store key and watch ids
    watch: HashMap<i64, KeyRange>,
    /// map to store watch id and watch response senders
    watch_response_sender: HashMap<i64, LockedDuplexSink>,
    /// set to store lock name to create_revision
    lock_map: HashMap<Vec<u8>, i64>,
    /// Sequence increasing watch id
    watch_id_counter: AtomicI64,
    /// Revision of etcd
    revision: AtomicI64,
    /// Lease id
    next_lease_id: i64,
    /// Lease 2 keyrangs
    lease_2_keyranges: HashMap<i64, BTreeMap<KeyRange, i64>>,
    /// Lease 2 lock keyrangs
    lease_2_lock_key: HashMap<i64, BTreeMap<Vec<u8>, i64>>,
    /// Lease - (timeout, need_renew, closed)
    lease_state: HashMap<i64, (i64, bool, bool)>,
}

/// A locked `DuplexSink` for watch response
type LockedDuplexSink = Arc<Mutex<DuplexSink<WatchResponse>>>;

/// Range end to get all keys
const ALL_KEYS: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];
impl MockEtcdInner {
    /// Create `MockEtcd`
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            watch: HashMap::new(),
            watch_response_sender: HashMap::new(),
            lock_map: HashMap::new(),
            watch_id_counter: AtomicI64::new(0),
            revision: AtomicI64::new(0),
            next_lease_id: 0,
            lease_2_keyranges: HashMap::new(),
            lease_2_lock_key: HashMap::new(),
            lease_state: HashMap::new(),
        }
    }

    /// Lease
    fn lease(&mut self, timeout_sec: u64) -> i64 {
        let lease_id = self.next_lease_id;
        self.next_lease_id = self.next_lease_id.overflow_add(1);
        self.lease_2_keyranges.insert(lease_id, BTreeMap::new());
        self.lease_2_lock_key.insert(lease_id, BTreeMap::new());
        self.lease_state.insert(
            lease_id,
            (
                timeout_sec
                    .try_into()
                    .unwrap_or_else(|e| panic!("timeout should < MAX_I64, err{e}")),
                false,
                false,
            ),
        );
        log::debug!("lease id allocated {}", lease_id);
        lease_id
    }

    /// Set Key range to lease
    fn set_key_range_2_lease(&mut self, lease_id: i64, range: KeyRange, key_revision: i64) -> bool {
        self.lease_2_keyranges
            .get_mut(&lease_id)
            .map_or(false, |set| {
                set.insert(range, key_revision);
                true
            })
    }
    /// Set lock key range to lease
    fn set_lock_key_2_lease(&mut self, lease_id: i64, key: Vec<u8>, key_revision: i64) -> bool {
        self.lease_2_lock_key
            .get_mut(&lease_id)
            .map_or(false, |set| {
                set.insert(key, key_revision);
                true
            })
    }

    /// Get kv by key vec
    fn map_get(&self, range_begin: &[u8], range_end: &[u8]) -> Vec<KeyValue> {
        let mut kvs = vec![];
        match range_end {
            ONE_KEY => {
                if let Some(kv) = self.map.get(range_begin) {
                    kvs.push(kv.clone());
                }
            }
            ALL_KEYS => {
                if *range_begin == vec![0_u8] {
                    self.map.values().for_each(|v| kvs.push(v.clone()));
                }
            }
            _ => {
                self.map.iter().for_each(|(k, v)| {
                    if k.as_slice() >= range_begin && k.as_slice() < range_end {
                        kvs.push(v.clone());
                    }
                });
            }
        }
        kvs
    }

    /// Get values of keys from a `RangeRequest` to map
    #[allow(clippy::pattern_type_mismatch)]
    #[inline]
    fn map_get_by_request(&self, req: &RangeRequest) -> Vec<KeyValue> {
        let key = req.get_key();
        let range_end = req.get_range_end();
        self.map_get(key, range_end)
    }

    /// Send watch response for a specific watch id to etcd client
    /// - return true if send success
    async fn send_watch_response_with_watch_id(
        &self,
        sender: LockedDuplexSink,
        kv: KeyValue,
        prev_kv: Option<KeyValue>,
        watch_id: i64,
        event_type: Event_EventType,
    ) -> Result<(), grpcio::Error> {
        let mut event = Event::new();
        event.set_field_type(event_type);
        if let Some(value) = prev_kv {
            event.set_prev_kv(value);
        }
        event.set_kv(kv);

        let mut response = WatchResponse::new();
        response.set_watch_id(watch_id);
        response.set_events(RepeatedField::from_vec([event].to_vec()));
        let header = response.mut_header();
        header.set_revision(self.revision.load(Ordering::Relaxed));

        sender
            .lock()
            .await
            .send((response, WriteFlags::default()))
            .await
    }

    /// Send watch response to etcd client
    #[allow(clippy::pattern_type_mismatch)]
    async fn send_watch_responses(
        &mut self,
        kv: KeyValue,
        prev_kv: Option<KeyValue>,
        event_type: Event_EventType,
    ) {
        let mut invalids: Vec<i64> = vec![];
        // Find all watch ids which watch this key and send watch response
        for (watch_id, v) in &self.watch {
            let sender = self
                .watch_response_sender
                .get(watch_id)
                .unwrap_or_else(|| panic!("Fail to get watch response sender from map"));
            match v.range_end.as_slice() {
                ONE_KEY => {
                    if v.key == kv.get_key()
                        && self
                            .send_watch_response_with_watch_id(
                                Arc::clone(sender),
                                kv.clone(),
                                prev_kv.clone(),
                                *watch_id,
                                event_type,
                            )
                            .await
                            .is_err()
                    {
                        invalids.push(*watch_id);
                    }
                }
                ALL_KEYS => {
                    if v.key == vec![0_u8]
                        && self
                            .send_watch_response_with_watch_id(
                                Arc::clone(sender),
                                kv.clone(),
                                prev_kv.clone(),
                                *watch_id,
                                event_type,
                            )
                            .await
                            .is_err()
                    {
                        invalids.push(*watch_id);
                    }
                }
                _ => {
                    if kv.get_key().to_vec() >= v.key
                        && kv.get_key().to_vec() < v.range_end
                        && self
                            .send_watch_response_with_watch_id(
                                Arc::clone(sender),
                                kv.clone(),
                                prev_kv.clone(),
                                *watch_id,
                                event_type,
                            )
                            .await
                            .is_err()
                    {
                        invalids.push(*watch_id);
                    }
                }
            }
        }
        for i in invalids {
            self.watch.remove(&i);
            self.watch_response_sender.remove(&i);
        }
    }

    /// Send watch response if client falls behind
    /// Etcd will send all changes from requested revision until latest revision.
    /// For `MockEtcd`, just latest revision is sent. This already meets test requirement.
    async fn catch_up_revision(
        &self,
        sender: LockedDuplexSink,
        watch_id: i64,
        watch_request: WatchRequest,
    ) {
        let request_revision = watch_request.get_create_request().get_start_revision();
        let request_key = watch_request.get_create_request().get_key().to_vec();

        if let Some(kv) = self.map.get(&request_key) {
            if request_revision < kv.get_mod_revision() {
                let mut event = Event::new();
                event.set_field_type(Event_EventType::PUT);
                event.set_kv(kv.clone());

                let mut response = WatchResponse::new();
                response.set_watch_id(watch_id);
                response.set_events(RepeatedField::from_vec([event].to_vec()));
                let header = response.mut_header();
                header.set_revision(self.revision.load(Ordering::Relaxed));

                sender
                    .lock()
                    .await
                    .send((response, WriteFlags::default()))
                    .await
                    .unwrap_or_else(|e| panic!("Fail to send watch response, the error is {}", e));
            }
        }
    }

    /// Insert a key value from a `PutRequest` to map
    async fn map_insert(&mut self, req: &PutRequest) -> Option<KeyValue> {
        let prev_revision = self.revision.fetch_add(1, Ordering::Relaxed);
        let mut kv = KeyValue::new();
        kv.set_key(req.get_key().to_vec());
        kv.set_value(req.get_value().to_vec());

        kv.set_mod_revision(prev_revision.overflow_add(1));
        let prev_kv = self.map.get(&req.get_key().to_vec()).cloned();
        if let Some(ref prev_kv) = prev_kv {
            kv.set_version(prev_kv.get_version().overflow_add(1));
        } else {
            kv.set_version(1);
            kv.set_create_revision(kv.mod_revision);
        }

        if !req.ignore_lease {
            self.set_key_range_2_lease(
                req.lease,
                KeyRange {
                    key: req.get_key().to_vec(),
                    range_end: vec![],
                },
                kv.create_revision,
            );
        }

        let insert_res = self.map.insert(req.get_key().to_vec(), kv.clone());
        self.send_watch_responses(kv.clone(), prev_kv, Event_EventType::PUT)
            .await;
        insert_res
    }

    /// Delete keys by key range from map
    async fn map_delete_by_keyrange(
        &mut self,
        begin: Vec<u8>,
        range_end: Vec<u8>,
        with_revision_check: Option<i64>,
    ) -> Vec<KeyValue> {
        let mut prev_kvs = vec![];

        let mut check_value_retain = |kv: &KeyValue| {
            if let Some(revision_check) = with_revision_check {
                if kv.create_revision == revision_check {
                    prev_kvs.push(kv.clone());
                    false
                } else {
                    true
                }
            } else {
                prev_kvs.push(kv.clone());
                false
            }
        };

        match range_end.as_slice() {
            ONE_KEY => {
                let remove_able = if let Some(revision_check) = with_revision_check {
                    self.map
                        .get(&begin)
                        .map_or(false, |kv| kv.create_revision == revision_check)
                } else {
                    true
                };

                if remove_able {
                    if let Some(kv) = self.map.remove(&begin) {
                        prev_kvs.push(kv);
                    }
                }
            }
            ALL_KEYS => {
                if begin == vec![0_u8] {
                    self.map.retain(|_k, v| check_value_retain(v));
                }
            }
            _ => {
                self.map.retain(|k, v| {
                    if k >= &begin && k < &range_end {
                        check_value_retain(v)
                    } else {
                        true
                    }
                });
            }
        }
        self.revision.fetch_add(1, Ordering::Relaxed);
        for kv in prev_kvs.clone() {
            self.send_watch_responses(kv.clone(), Some(kv.clone()), Event_EventType::DELETE)
                .await;
        }
        prev_kvs
    }

    /// Delete keys from `DeleteRangeRequest` from map
    #[allow(clippy::pattern_type_mismatch)]
    #[inline]
    async fn map_delete(
        &mut self,
        req: &DeleteRangeRequest,
        with_revision_check: Option<i64>,
    ) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        self.map_delete_by_keyrange(key, range_end, with_revision_check)
            .await
    }
}
impl MockEtcd {
    /// Create `MockEtcd`
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockEtcdInner::new())),
        }
    }

    /// Handle `RangeRequest`
    fn range_inner(inner: &mut MutexGuard<'_, MockEtcdInner>, req: &RangeRequest) -> RangeResponse {
        let kvs = inner.map_get_by_request(req);
        let mut response = RangeResponse::new();
        response.set_count(kvs.len().cast());
        response.set_kvs(RepeatedField::from_vec(kvs));
        let header = response.mut_header();
        header.set_revision(inner.revision.load(Ordering::Relaxed));
        response
    }

    /// Handle `PutRequest`
    async fn put_inner(inner: &mut MutexGuard<'_, MockEtcdInner>, req: &PutRequest) -> PutResponse {
        let mut response = PutResponse::new();
        let prev = inner.map_insert(req).await;
        if let Some(kv) = prev {
            response.set_prev_kv(kv);
        }
        let header = response.mut_header();
        header.set_revision(inner.revision.load(Ordering::Relaxed));
        response
    }

    /// Handle `DeleteRangeRequest`
    async fn delete_range_inner(
        inner: &mut MutexGuard<'_, MockEtcdInner>,
        req: &DeleteRangeRequest,
    ) -> DeleteRangeResponse {
        let mut response = DeleteRangeResponse::new();
        let get_prev = req.get_prev_kv();
        let prev_kvs = inner.map_delete(req, None).await;
        response.set_deleted(prev_kvs.len().cast());
        if get_prev {
            response.set_prev_kvs(RepeatedField::from_vec(prev_kvs));
        }
        let header = response.mut_header();
        header.set_revision(inner.revision.load(Ordering::Relaxed));

        response
    }

    /// Execute txn operations after compare
    #[async_recursion]
    async fn handle_txn_ops(
        inner_locked: &mut MutexGuard<'_, MockEtcdInner>,
        reqs: &RepeatedField<RequestOp>,
    ) -> Vec<ResponseOp> {
        let mut responses = vec![];
        for ope in reqs {
            if ope.has_request_delete_range() {
                let mut resp = ResponseOp::new();
                resp.set_response_delete_range(
                    Self::delete_range_inner(inner_locked, ope.get_request_delete_range()).await,
                );
                responses.push(resp);
            } else if ope.has_request_put() {
                let mut resp = ResponseOp::new();
                resp.set_response_put(Self::put_inner(inner_locked, ope.get_request_put()).await);
                responses.push(resp);
            } else if ope.has_request_range() {
                let mut resp = ResponseOp::new();
                resp.set_response_range(Self::range_inner(inner_locked, ope.get_request_range()));
                responses.push(resp);
            } else {
                assert!(
                    ope.has_request_txn(),
                    "txn operation should be one of put, delete_range, range, txn"
                );
                let mut resp = ResponseOp::new();
                resp.set_response_txn(Self::txn_inner(inner_locked, ope.get_request_txn()).await);
                responses.push(resp);
            }
        }
        responses
    }

    /// Handle `TxnRequest`
    async fn txn_inner(inner: &mut MutexGuard<'_, MockEtcdInner>, req: &TxnRequest) -> TxnResponse {
        let mut response = TxnResponse::new();
        response.succeeded = true;

        for compare in &req.compare {
            let begin = compare.get_key();
            let end = compare.get_range_end();
            let kvs = inner.map_get(begin, end);
            // One compare can only match at most one key
            assert!(kvs.len() < 2);
            if compare.has_create_revision() {
                let create_revision = compare.get_create_revision();
                if create_revision == 0 {
                    // Expected no key, but key exists
                    if !kvs.is_empty() {
                        response.succeeded = false;
                    }
                } else {
                    // Expected key, but key doesn't exist
                    if let Some(kv) = kvs.get(0) {
                        response.succeeded = create_revision == kv.get_create_revision();
                    } else {
                        response.succeeded = false;
                    }
                }
            } else if compare.has_mod_revision() {
                let mod_revision = compare.get_mod_revision();
                if mod_revision == 0 {
                    // Expected no key, but key exists
                    if !kvs.is_empty() {
                        response.succeeded = false;
                    }
                } else {
                    // Expected key, but key doesn't exist
                    if let Some(kv) = kvs.get(0) {
                        response.succeeded = mod_revision == kv.get_mod_revision();
                    } else {
                        response.succeeded = false;
                    }
                }
            } else if compare.has_value() {
                if let Some(kv) = kvs.get(0) {
                    let value = compare.get_value();
                    response.succeeded = value == kv.get_value();
                } else {
                    response.succeeded = false;
                }
            } else {
                assert!(compare.has_version());
                // if compare.has_version()
                let version = compare.get_version();
                if version == 0 {
                    // Expected no key, but key exists
                    if !kvs.is_empty() {
                        response.succeeded = false;
                    }
                } else {
                    // Expected key, but key doesn't exist
                    if let Some(kv) = kvs.get(0) {
                        response.succeeded = version == kv.get_version();
                    } else {
                        response.succeeded = false;
                    }
                }
            }
        }
        if response.succeeded {
            response.responses =
                RepeatedField::from_vec(Self::handle_txn_ops(inner, &req.success).await);
        } else {
            response.responses =
                RepeatedField::from_vec(Self::handle_txn_ops(inner, &req.failure).await);
        }

        response
    }
}

impl Watch for MockEtcd {
    fn watch(
        &mut self,
        _ctx: RpcContext,
        mut stream: RequestStream<WatchRequest>,
        sink: DuplexSink<WatchResponse>,
    ) {
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let sink_arc = Arc::new(Mutex::new(sink));
            while let Some(request) = stream.next().await {
                let mut inner = inner_clone.lock().await;
                match request {
                    Ok(watch_request) => {
                        if watch_request.has_create_request() {
                            let key_range = KeyRange {
                                key: watch_request.get_create_request().get_key().to_vec(),
                                range_end: watch_request
                                    .get_create_request()
                                    .get_range_end()
                                    .to_vec(),
                            };
                            let watch_id = inner.watch_id_counter.fetch_add(1, Ordering::Relaxed);
                            inner.watch.insert(watch_id, key_range);
                            inner
                                .watch_response_sender
                                .insert(watch_id, Arc::clone(&sink_arc));
                            let mut response = WatchResponse::new();
                            response.set_watch_id(watch_id);
                            response.set_created(true);

                            sink_arc
                                .lock()
                                .await
                                .send((response, WriteFlags::default()))
                                .await
                                .unwrap_or_else(|e| {
                                    panic!("Fail to send watch response, the error is {}", e)
                                });
                            inner
                                .catch_up_revision(Arc::clone(&sink_arc), watch_id, watch_request)
                                .await;
                        } else {
                            let watch_id = watch_request.get_cancel_request().get_watch_id();
                            inner.watch.remove(&watch_id);
                            inner.watch_response_sender.remove(&watch_id);

                            let mut response = WatchResponse::new();
                            response.set_watch_id(watch_id);
                            response.set_canceled(true);

                            sink_arc
                                .lock()
                                .await
                                .send((response, WriteFlags::default()))
                                .await
                                .unwrap_or_else(|e| {
                                    panic!("Fail to send watch response, the error is {}", e)
                                });
                        }
                    }
                    Err(e) => {
                        warn!("Fail to receive watch request, the error is: {}", e);
                        break;
                    }
                }
            }
        };

        smol::spawn(task).detach();
    }
}

impl Kv for MockEtcd {
    fn range(&mut self, _ctx: RpcContext, req: RangeRequest, sink: UnarySink<RangeResponse>) {
        debug!(
            "Receive range request key={:?}, range_end={:?}",
            req.get_key(),
            req.get_range_end()
        );
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;
            success(Self::range_inner(&mut inner, &req), sink, "range_success").await;
        };

        smol::spawn(task).detach();
    }

    fn put(&mut self, _ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        debug!(
            "Receive put request key={:?}, value={:?}",
            req.get_key(),
            req.get_value()
        );

        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;
            success(Self::put_inner(&mut inner, &req).await, sink, "put_success").await;
        };
        smol::spawn(task).detach();
    }

    fn delete_range(
        &mut self,
        _ctx: RpcContext,
        req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        debug!(
            "Receive delete range request key={:?}, range_end={:?}",
            req.get_key(),
            req.get_range_end()
        );

        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;

            debug!(
                "Handling delete range request, inner locked, key={:?}, range_end={:?}",
                req.get_key(),
                req.get_range_end()
            );

            let response = Self::delete_range_inner(&mut inner, &req).await;

            // sometimes it can't send the response, so we use a timer to make sure the response is sent
            Timer::after(Duration::from_millis(1000));
            success(response, sink, "delete success").await;
        };
        smol::spawn(task).detach();
    }

    fn txn(&mut self, _ctx: RpcContext, req: TxnRequest, sink: UnarySink<TxnResponse>) {
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        debug!("Receive txn request");
        smol::spawn(async move {
            let mut inner = inner_clone.lock().await;
            let response = Self::txn_inner(&mut inner, &req).await;
            success(response, sink, "txn success").await;
        })
        .detach();
    }

    fn compact(
        &mut self,
        ctx: RpcContext,
        _req: CompactionRequest,
        sink: UnarySink<CompactionResponse>,
    ) {
        let msg = "Compact Not Implemented";
        log::debug!("{msg}");
        fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, msg.to_owned());
    }
}

impl Lock for MockEtcd {
    fn lock(&mut self, _ctx: RpcContext, req: LockRequest, sink: UnarySink<LockResponse>) {
        debug!("Receive lock request key={:?}", req.get_name(),);

        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            loop {
                let mut inner = inner_clone.lock().await;

                if inner.lock_map.get(req.get_name()).is_some() {
                    drop(inner);
                    Timer::after(Duration::from_millis(300)).await;
                } else {
                    let prev_revision = inner.revision.fetch_add(1, Ordering::Relaxed);

                    let mut response = LockResponse::new();
                    response.set_key(req.get_name().to_vec());
                    let header = response.mut_header();
                    header.set_revision(prev_revision);
                    match sink.success(response).await {
                        Ok(_) => {
                            log::debug!(
                                "lock key:{:?} with lease:{}",
                                req.get_name(),
                                req.get_lease()
                            );
                            inner
                                .lock_map
                                .insert(req.get_name().to_vec(), prev_revision.overflow_add(1));
                            inner.set_lock_key_2_lease(
                                req.get_lease(),
                                req.get_name().to_vec(),
                                prev_revision.overflow_add(1),
                            );
                        }
                        Err(e) => {
                            warn!("Fail to send lock response, the error is: {}", e);
                        }
                    }
                    drop(inner);
                    break;
                }
            }
        };

        smol::spawn(task).detach();
    }

    fn unlock(&mut self, _ctx: RpcContext, req: UnlockRequest, sink: UnarySink<UnlockResponse>) {
        debug!("Receive unlock request key={:?}", req.get_key(),);
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;
            let revision = inner.revision.load(Ordering::Relaxed);
            if inner.lock_map.get(req.get_key()).is_some() {
                log::debug!("unlock remove key {:?}", req.get_key());
                inner.revision.fetch_add(1, Ordering::Relaxed);
                inner.lock_map.remove(req.get_key());
            } else {
            }

            drop(inner);

            let mut response = UnlockResponse::new();
            let header = response.mut_header();
            header.set_revision(revision);
            success(response, sink, "unlock success").await;
        };

        smol::spawn(task).detach();
    }
}

impl Lease for MockEtcd {
    fn lease_grant(
        &mut self,
        _ctx: RpcContext,
        req: LeaseGrantRequest,
        sink: UnarySink<LeaseGrantResponse>,
    ) {
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;
            let prev_revision = inner.revision.fetch_add(1, Ordering::Relaxed);
            let timeout_sec = req.TTL.try_into().unwrap_or_else(|e| {
                panic!(
                    "lease ttl should be u64, but got < 0: {:?}, err:{e}",
                    req.TTL
                )
            });
            let lease_id = inner.lease(timeout_sec);
            drop(inner);

            smol::spawn(async move {
                // ttl here is second
                // https://docs.rs/etcd-rs/latest/src/etcd_rs/lease/grant.rs.html#14-21
                {
                    let mut timecount = timeout_sec;
                    while timecount > 0 {
                        Timer::after(Duration::from_secs(1)).await;
                        timecount = timecount.overflow_sub(1);
                        let mut inner = inner_clone.lock().await;
                        let &mut (_, ref mut need_renew, ref close) = inner
                            .lease_state
                            .get_mut(&lease_id)
                            .unwrap_or_else(|| panic!("lease id not found: {lease_id}"));
                        if *close {
                            break;
                        } else if *need_renew {
                            log::debug!("lease {} keep alived", lease_id);
                            timecount = timeout_sec;
                            *need_renew = false;
                        } else {
                        }
                    }
                }
                log::debug!("lease timeout {}", lease_id);
                let mut inner = inner_clone.lock().await;

                let keyranges = inner
                    .lease_2_keyranges
                    .remove(&lease_id)
                    .unwrap_or_else(|| panic!("lease id not found: {lease_id}"));
                for (keyrange, revision) in &keyranges {
                    log::debug!("lease timeout {} remove key {:?}", lease_id, keyrange);
                    let begin = keyrange.key.clone();
                    let end = keyrange.range_end.clone();
                    inner
                        .map_delete_by_keyrange(begin, end, Some(*revision))
                        .await;
                }

                let keys = inner
                    .lease_2_lock_key
                    .remove(&lease_id)
                    .unwrap_or_else(|| panic!("lease id not found: {lease_id}"));
                for (key, revision) in &keys {
                    log::debug!("lease timeout {} remove lock key {:?}", lease_id, key);
                    let removeable = inner
                        .lock_map
                        .get(key)
                        .map_or(false, |lock_revision| lock_revision == revision);
                    if removeable {
                        inner.lock_map.remove(key);
                    }
                }

                let _ = inner
                    .lease_state
                    .remove(&lease_id)
                    .unwrap_or_else(|| panic!("lease id not found: {lease_id}"));
            })
            .detach();
            let mut response = LeaseGrantResponse::new();
            response.mut_header().revision = prev_revision;
            response.ID = lease_id;
            success(response, sink, "lease grant success").await;
        };

        smol::spawn(task).detach();
    }

    fn lease_revoke(
        &mut self,
        _ctx: RpcContext,
        req: LeaseRevokeRequest,
        sink: UnarySink<LeaseRevokeResponse>,
    ) {
        debug!("Receive lease revoke, lease id={:?}", req.ID,);
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            let mut inner = inner_clone.lock().await;

            if let Some(&mut (_, _, ref mut close)) = inner.lease_state.get_mut(&req.ID) {
                *close = true;
            }

            let revision = inner.revision.load(Ordering::Relaxed);
            drop(inner);

            let mut response = LeaseRevokeResponse::new();
            let header = response.mut_header();
            header.set_revision(revision);
            success(response, sink, "lease revoke success").await;
        };

        smol::spawn(task).detach();
    }

    fn lease_keep_alive(
        &mut self,
        _ctx: RpcContext,
        mut req: RequestStream<LeaseKeepAliveRequest>,
        mut sink: DuplexSink<LeaseKeepAliveResponse>,
    ) {
        let inner_clone = Arc::<Mutex<MockEtcdInner>>::clone(&self.inner);
        let task = async move {
            while let Some(req) = req.next().await {
                match req {
                    Ok(req) => {
                        let mut inner = inner_clone.lock().await;
                        if let Some(&mut (ref mut timeout, ref mut need_renew, _)) =
                            inner.lease_state.get_mut(&req.get_ID())
                        {
                            let mut response = LeaseKeepAliveResponse::new();
                            response.set_ID(req.get_ID());
                            response.set_TTL(*timeout);
                            *need_renew = true;
                            if let Err(err) = sink.send((response, WriteFlags::default())).await {
                                log::warn!("lease keep alive sink send error: {err:?}");
                                break;
                            }
                        } else {
                            log::warn!(
                                "lease {} doesn't exist, end lease keep alive stream",
                                req.get_ID()
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        log::debug!("lease keep alive stream recv error: {:?}", e);
                        break;
                    }
                }
            }
            if let Err(err) = sink.close().await {
                log::warn!("lease keep alive stream close sink error: {err:?}");
            }
            log::debug!("end lease keep alive stream");
        };

        smol::spawn(task).detach();
    }

    fn lease_time_to_live(
        &mut self,
        ctx: RpcContext,
        _req: LeaseTimeToLiveRequest,
        sink: UnarySink<LeaseTimeToLiveResponse>,
    ) {
        let msg = "Lease Time To Live Not Implemented";
        log::debug!("{msg}");
        fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, msg.to_owned());
    }
}

#[cfg(test)]
#[allow(clippy::all, clippy::restriction)]
#[allow(clippy::too_many_lines)]
mod test {
    use crate::{mock_etcd::MockEtcd, MockEtcdServer};
    use async_lock::Mutex;
    use etcd_client::{
        Client, ClientConfig, EtcdDeleteRequest, EtcdLeaseGrantRequest, EtcdLeaseKeepAliveRequest,
        EtcdLeaseRevokeRequest, EtcdLockRequest, EtcdPutRequest, EtcdRangeRequest,
        EtcdUnlockRequest, KeyRange, TxnCmp,
    };
    use futures::FutureExt;
    use std::{
        env::set_var,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn timestamp() -> Duration {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|err| panic!("Time went backwards, err:{err}"))
    }

    #[test]
    fn test_all() {
        set_var("RUST_LOG", "debug");
        env_logger::init();

        unit_test();
        let mut etcd_server = MockEtcdServer::new();
        etcd_server.start();
        let client = Arc::new(smol::future::block_on(async {
            let endpoints = vec!["127.0.0.1:2379".to_owned()];
            let config = ClientConfig::new(endpoints, None, 10, false);
            let client = Client::connect(config).await.unwrap_or_else(|err| {
                panic!("failed to connect to etcd server, the error is: {}", err)
            });
            client
        }));
        {
            let client = client.clone();
            smol::future::block_on(async {
                client
                    .kv()
                    .delete(EtcdDeleteRequest::new(KeyRange::all()))
                    .await
                    .unwrap();
            });
        }
        e2e_test(&client.clone());
        e2e_watch_test(&client.clone());
        e2e_lock_lease_test(&client.clone());
        e2e_txn_test(&client.clone());
        smol::future::block_on(async {
            client
                .shutdown()
                .await
                .unwrap_or_else(|e| panic!("failed to shutdown client, the error is {}", e));
        });
    }
    fn unit_test() {
        smol::future::block_on(async {
            let mock_etcd = MockEtcd::new();
            let mut inner = mock_etcd.inner.lock().await;
            // Test insert
            let mut put000 = crate::rpc::PutRequest::new();
            let mut put001 = crate::rpc::PutRequest::new();
            let mut put010 = crate::rpc::PutRequest::new();
            let mut put011 = crate::rpc::PutRequest::new();
            let mut put100 = crate::rpc::PutRequest::new();
            let mut put101 = crate::rpc::PutRequest::new();
            let mut put110 = crate::rpc::PutRequest::new();
            let mut put111 = crate::rpc::PutRequest::new();
            put000.set_key(vec![0_u8, 0_u8, 0_u8]);
            put001.set_key(vec![0_u8, 0_u8, 1_u8]);
            put010.set_key(vec![0_u8, 1_u8, 0_u8]);
            put011.set_key(vec![0_u8, 1_u8, 1_u8]);
            put100.set_key(vec![1_u8, 0_u8, 0_u8]);
            put101.set_key(vec![1_u8, 0_u8, 1_u8]);
            put110.set_key(vec![1_u8, 1_u8, 0_u8]);
            put111.set_key(vec![1_u8, 1_u8, 1_u8]);
            put000.set_value(vec![0_u8, 0_u8, 0_u8]);
            put001.set_value(vec![0_u8, 0_u8, 1_u8]);
            put010.set_value(vec![0_u8, 1_u8, 0_u8]);
            put011.set_value(vec![0_u8, 1_u8, 1_u8]);
            put100.set_value(vec![1_u8, 0_u8, 0_u8]);
            put101.set_value(vec![1_u8, 0_u8, 1_u8]);
            put110.set_value(vec![1_u8, 1_u8, 0_u8]);
            put111.set_value(vec![1_u8, 1_u8, 1_u8]);

            assert_eq!(inner.map_insert(&put000).await, None);

            assert_eq!(inner.map_insert(&put001).await, None);
            assert_eq!(inner.map_insert(&put010).await, None);
            assert_eq!(inner.map_insert(&put011).await, None);
            assert_eq!(inner.map_insert(&put100).await, None);
            assert_eq!(inner.map_insert(&put101).await, None);
            assert_eq!(inner.map_insert(&put110).await, None);
            assert_eq!(inner.map_insert(&put111).await, None);
            assert_eq!(
                {
                    let kv = inner.map_insert(&put000).await;
                    kv.unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            // Test get
            // get one key
            let mut one_key_1 = crate::rpc::RangeRequest::new();
            one_key_1.set_key(vec![0_u8]);
            one_key_1.set_range_end(vec![]);
            let mut one_key_2 = crate::rpc::RangeRequest::new();
            one_key_2.set_key(vec![0_u8, 0_u8, 0_u8]);
            one_key_2.set_range_end(vec![]);
            // get all keys
            let mut all_keys = crate::rpc::RangeRequest::new();
            all_keys.set_key(vec![0_u8]);
            all_keys.set_range_end(vec![0_u8]);
            // get range
            let mut range1 = crate::rpc::RangeRequest::new();
            range1.set_key(vec![0_u8, 0_u8, 0_u8]);
            range1.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            let mut range2 = crate::rpc::RangeRequest::new();
            range2.set_key(vec![0_u8, 0_u8, 0_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);
            let mut range2 = crate::rpc::RangeRequest::new();
            range2.set_key(vec![0_u8, 1_u8, 1_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);

            assert_eq!(inner.map_get_by_request(&one_key_1), vec![]);
            assert_eq!(
                {
                    let kv = inner.map_get_by_request(&one_key_2);
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            assert_eq!(inner.map_get_by_request(&all_keys).len(), 8);
            assert_eq!(inner.map_get_by_request(&range1).len(), 2);
            assert_eq!(inner.map_get_by_request(&range2).len(), 4);

            // Test delete
            let mut delete_no_exist = crate::rpc::DeleteRangeRequest::new();
            delete_no_exist.set_key(vec![0_u8]);
            delete_no_exist.set_range_end(vec![]);

            let mut delete_one_key = crate::rpc::DeleteRangeRequest::new();
            delete_one_key.set_key(vec![1_u8, 1_u8, 1_u8]);
            delete_one_key.set_range_end(vec![]);
            // delete range
            let mut delete_range = crate::rpc::DeleteRangeRequest::new();
            delete_range.set_key(vec![0_u8, 0_u8, 0_u8]);
            delete_range.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            // delete all
            let mut delete_all = crate::rpc::DeleteRangeRequest::new();
            delete_all.set_key(vec![0_u8]);
            delete_all.set_range_end(vec![0_u8]);

            assert_eq!(inner.map_delete(&delete_no_exist, None).await.len(), 0);
            assert_eq!(
                {
                    let kv = inner.map_delete(&delete_one_key, None).await;
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![1_u8, 1_u8, 1_u8]
            );
            assert_eq!(inner.map.len(), 7);
            assert_eq!(inner.map_delete(&delete_range, None).await.len(), 2);
            assert_eq!(inner.map.len(), 5);
            assert_eq!(inner.map_delete(&delete_all.clone(), None).await.len(), 5);
            assert_eq!(inner.map.len(), 0);
            // test kv revision
            inner.map_insert(&put000).await;
            let prev_val1 = inner.map_insert(&put000).await;
            let revision1 = prev_val1.unwrap().get_mod_revision();
            let prev_val2 = inner.map_insert(&put000).await;
            let revision2 = prev_val2.unwrap().get_mod_revision();
            assert_eq!(revision1 + 1, revision2);
        });
    }

    fn e2e_test(client: &Arc<Client>) {
        log::debug!("start e2e test");
        smol::future::block_on(async {
            let key000 = vec![0_u8, 0_u8, 0_u8];
            let key001 = vec![0_u8, 0_u8, 1_u8];
            let key010 = vec![0_u8, 1_u8, 0_u8];
            let key011 = vec![0_u8, 1_u8, 1_u8];
            let key100 = vec![1_u8, 0_u8, 0_u8];
            let key101 = vec![1_u8, 0_u8, 1_u8];
            let key110 = vec![1_u8, 1_u8, 0_u8];
            let key111 = vec![1_u8, 1_u8, 1_u8];

            client
                .kv()
                .put(EtcdPutRequest::new(key000.clone(), key000.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key000, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key001.clone(), key001.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key001, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key010.clone(), key010.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key010, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key011.clone(), key011.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key011, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key100.clone(), key100.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key100, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key101.clone(), key101.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key101, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key110.clone(), key110.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key110, the error is {}", err)
                });
            client
                .kv()
                .put(EtcdPutRequest::new(key111.clone(), key111.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key111, the error is {}", err)
                });

            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::key(vec![0_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
            assert_eq!(resp.count(), 0);
            let mut resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::key(key000.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 000, the error is {}", err));
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.take_kvs().get(0).unwrap().value(), key000);

            let mut resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::key(key111.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 111, the error is {}", err));
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.take_kvs().get(0).unwrap().value(), key111);

            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::range(
                    key000.clone(),
                    key100,
                )))
                .await
                .unwrap_or_else(|err| panic!("failed to get range 000-100, the error is {}", err));
            assert_eq!(resp.count(), 4);
            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::all()))
                .await
                .unwrap_or_else(|err| panic!("failed to get range all, the error is {}", err));
            assert_eq!(resp.count(), 8);
            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::prefix(vec![1_u8, 1_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to get prefix 11, the error is {}", err));
            assert_eq!(resp.count(), 2);

            let resp = client
                .kv()
                .delete(EtcdDeleteRequest::new(KeyRange::key(vec![0_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to delete key 0, the error is {}", err));
            assert_eq!(resp.count_deleted(), 0);

            let mut delete_req = EtcdDeleteRequest::new(KeyRange::key(key000.clone()));
            delete_req.set_prev_kv(true);
            let mut resp = client
                .kv()
                .delete(delete_req)
                .await
                .unwrap_or_else(|err| panic!("failed to delete key 000, the error is {}", err));
            assert_eq!(resp.take_prev_kvs().get(0).unwrap().value(), key000);

            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::key(key000)))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 000, the error is {}", err));
            assert_eq!(resp.count(), 0);

            let resp = client
                .kv()
                .delete(EtcdDeleteRequest::new(KeyRange::prefix(vec![1_u8, 1_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to delete prefix 11, the error is {}", err));
            assert_eq!(resp.count_deleted(), 2);

            let resp = client
                .kv()
                .range(EtcdRangeRequest::new(KeyRange::key(key111.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 111, the error is {}", err));
            assert_eq!(resp.count(), 0);
        });
    }

    #[allow(dead_code)]
    fn e2e_watch_test(client: &Arc<Client>) {
        log::debug!("start e2e_watch_test");
        smol::future::block_on(async {
            let key000 = vec![0_u8, 0_u8, 0_u8];
            let key001 = vec![0_u8, 0_u8, 1_u8];
            let key100 = vec![1_u8, 0_u8, 0_u8];
            let key101 = vec![1_u8, 0_u8, 1_u8];
            let key110 = vec![1_u8, 1_u8, 0_u8];

            let test_data = vec![
                key110,
                key000.clone(),
                key001.clone(),
                key100.clone(),
                key101,
            ];

            let mut resp_receiver = client
                .watch(KeyRange::range(key000.clone(), key100.clone()))
                .await
                .unwrap_or_else(|e| panic!("watch failed, err {e}"));

            for key in test_data {
                client
                    .kv()
                    .put(EtcdPutRequest::new(key.clone(), key.clone()))
                    .await
                    .unwrap_or_else(|err| panic!("failed to put key-value, the error is {}", err));
            }

            let mut delete_req = EtcdDeleteRequest::new(KeyRange::key(key000.clone()));
            delete_req.set_prev_kv(true);
            let mut resp = client
                .kv()
                .delete(delete_req)
                .await
                .unwrap_or_else(|err| panic!("failed to delete key 000, the error is {}", err));
            assert_eq!(resp.take_prev_kvs().get(0).unwrap().value(), key000);

            for _x in 0..3 {
                match resp_receiver.recv().await {
                    Ok(mut watch_resp) => {
                        let mut events = watch_resp.take_events();
                        let event = events
                            .get_mut(0)
                            .unwrap_or_else(|| panic!("Fail to take events from watch response"));
                        let kv = event
                            .take_kvs()
                            .unwrap_or_else(|| panic!("Fail to take kvs from watch response"))
                            .take_key()
                            .clone();
                        assert!((kv == key000.clone()) || (kv == key001.clone()));
                    }
                    Err(e) => {
                        panic!("failed to receive watch response, the error is {}", e)
                    }
                }
            }
        });
    }

    fn e2e_lock_lease_test(client: &Arc<Client>) {
        log::debug!("start e2e_lock_lease_test");
        smol::future::block_on(async {
            async fn test_lock_basic(client: Client) {
                let lock_key012 = vec![0_u8, 1_u8, 2_u8];
                let res = client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                        5,
                    )))
                    .await
                    .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                let lease_id = res.id();

                // lock unlock twice
                {
                    log::debug!("test basic lock");
                    let mut res = client
                        .lock()
                        .lock(EtcdLockRequest::new(lock_key012.clone(), lease_id))
                        .await
                        .unwrap_or_else(|err| {
                            panic!("failed to lock key012, the error is {}", err)
                        });

                    client
                        .lock()
                        .unlock(EtcdUnlockRequest::new(res.take_key()))
                        .await
                        .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));

                    let mut res = client
                        .lock()
                        .lock(EtcdLockRequest::new(lock_key012.clone(), lease_id))
                        .await
                        .unwrap_or_else(|err| {
                            panic!("failed to lock key012, the error is {}", err)
                        });

                    client
                        .lock()
                        .unlock(EtcdUnlockRequest::new(res.take_key()))
                        .await
                        .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                }
            }
            async fn test_lock_lease(client: Client) {
                let lock_key = "test_lock_lease";
                // new lease id for new lock
                let res = client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                        5,
                    )))
                    .await
                    .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                let lease_id = res.id();

                log::debug!("test lock with lease");
                // lock first and the second lock can be return in 5s.
                let _res = client
                    .lock()
                    .lock(EtcdLockRequest::new(lock_key, lease_id))
                    .await
                    .unwrap_or_else(|err| panic!("failed to lock key012, the error is {}", err));

                // new lease id for new lock
                let res = client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                        10,
                    )))
                    .await
                    .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                let lease_id = res.id();

                let locked = timestamp();
                let mut lock = client.lock();
                let res = futures::select! {
                    _ = smol::Timer::after(Duration::from_secs(7)).fuse() => {
                        let timeout=timestamp();
                        log::debug!("wait for 8s and lock didn't return, {}",(timeout-locked).as_secs());
                        None
                    }
                    res  = lock.lock(EtcdLockRequest::new(lock_key, lease_id)).fuse() => Some(res.unwrap())
                };
                let return_time = timestamp();
                log::debug!(
                    "wait for lock return cost {} ms",
                    (return_time - locked).as_millis()
                );
                // lease is smaller the 5s now and lock will success in about 5s
                assert!(
                    res.is_some(),
                    "lease is smaller than 5s now and lock should return in about 5s"
                );

                {
                    for _ in 0..3 {
                        log::debug!("send keep alive request");
                        // keep alive the lease
                        client
                            .lease()
                            .keep_alive(EtcdLeaseKeepAliveRequest::new(lease_id))
                            .await
                            .unwrap();

                        smol::Timer::after(Duration::from_secs(1)).await;
                    }

                    let begin_lock = timestamp();
                    lock.lock(EtcdLockRequest::new(lock_key, lease_id))
                        .await
                        .unwrap();
                    let end_lock = timestamp();
                    assert!(
                        (end_lock - begin_lock).as_secs() > 4,
                        "lock should return in about 5s, because we keep alive the lease",
                    );
                }
            }
            async fn test_lock_cancel(client: Client) {
                let lock_key = "test_lock_cancel";
                // lock will be hold in 30s
                let res = client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                        30,
                    )))
                    .await
                    .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                let lease_id = res.id();

                log::debug!("test lock with lease");
                // lock first and the second lock can be return in 5s.
                let mut res = client
                    .lock()
                    .lock(EtcdLockRequest::new(lock_key, lease_id))
                    .await
                    .unwrap_or_else(|err| panic!("failed to lock key012, the error is {}", err));

                let mut client_lock = client.lock();
                futures::select! {
                    _=smol::Timer::after(Duration::from_secs(3)).fuse()=>{
                        log::debug!("cancel lock");
                    }
                    _=client_lock
                        .lock(EtcdLockRequest::new(lock_key, lease_id)).fuse()=>{
                        panic!("lock won't be release in 3s")
                    }
                }

                //Unlock should be successful because previous lock has not been released.
                client
                    .lock()
                    .unlock(EtcdUnlockRequest::new(res.take_key()))
                    .await
                    .expect(
                        "Unlock should be successful because previous lock has not been released.",
                    );
            }
            async fn test_lease_revoke(client: Client) {
                async fn lock(client: &Client, lock_key: &str) -> (u64, Vec<u8>) {
                    let res = client
                        .lease()
                        .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                            30,
                        )))
                        .await
                        .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
                    let lease_id = res.id();

                    // lock first and the second lock can be return in 5s.
                    let mut res = client
                        .lock()
                        .lock(EtcdLockRequest::new(lock_key, lease_id))
                        .await
                        .unwrap_or_else(|err| {
                            panic!("failed to lock key012, the error is {}", err)
                        });
                    (lease_id, res.take_key())
                }
                async fn unlock(client: &Client, lock_token: Vec<u8>) {
                    client
                        .lock()
                        .unlock(EtcdUnlockRequest::new(lock_token))
                        .await
                        .unwrap();
                }
                log::debug!("test_lease_revoke");
                let lock_key = "test_lease_revoke";
                // new lease id for new lock

                let (lease_id, _lock_token) = lock(&client, lock_key).await;
                client
                    .lease()
                    .revoke(EtcdLeaseRevokeRequest::new(lease_id))
                    .await
                    .unwrap();

                let lock_start = timestamp();
                let (lease_id, lock_token): (u64, Vec<u8>) = lock(&client, lock_key).await;
                let lock_end = timestamp();
                assert!((lock_end - lock_start).as_millis() < 2000);
                unlock(&client, lock_token).await;

                // lock same key with new lease, and revoke old lease, check if the old lease has effect on new lock.
                let (_, lock_token) = lock(&client, lock_key).await;

                client
                    .lease()
                    .revoke(EtcdLeaseRevokeRequest::new(lease_id))
                    .await
                    .unwrap();

                // lock should still be held and this lock will timeout.

                futures::select! {
                    _=smol::Timer::after(Duration::from_secs(1)).fuse()=>{}
                    _=lock(&client, lock_key).fuse()=>{
                        panic!("this lock shouldn't be get because the prev lock can't be unlocked by older lease")
                    }
                }
                unlock(&client, lock_token).await;
            }
            let tasks = vec![
                smol::spawn(test_lock_basic(client.as_ref().clone())),
                smol::spawn(test_lock_lease(client.as_ref().clone())),
                smol::spawn(test_lock_cancel(client.as_ref().clone())),
                smol::spawn(test_lease_revoke(client.as_ref().clone())),
            ];
            futures::future::join_all(tasks).await;
        });
    }

    fn e2e_txn_test(client: &Arc<Client>) {
        log::debug!("start e2e_txn_test");
        smol::future::block_on(async {
            let succ_put_value = Arc::new(Mutex::new("".to_owned()));
            let mut tasks = vec![];
            for txn_task_i in 0..5 {
                let client = client.clone();
                let succ_put_value = succ_put_value.clone();
                tasks.push(smol::spawn(async move {
                    let key = "txn_key";
                    let value = format!("txn_value{txn_task_i}");
                    let put_request = etcd_client::EtcdPutRequest::new(key.clone(), &*value);
                    let txn_req = etcd_client::EtcdTxnRequest::new()
                        .when_create_revision(
                            etcd_client::KeyRange::key(key.clone()),
                            TxnCmp::Equal,
                            0,
                        )
                        //key does not exist, insert kv
                        .and_then(put_request)
                        //key exists, return old value
                        .or_else(etcd_client::EtcdRangeRequest::new(
                            etcd_client::KeyRange::key(key.clone()),
                        ));
                    let txn_res = client.kv().txn(txn_req).await.unwrap();
                    if txn_res.is_success() {
                        let mut succ_put_value = succ_put_value.lock().await;
                        *succ_put_value = value.clone();
                    } else {
                        let mut txn_res = txn_res.get_responses();
                        assert_eq!(txn_res.len(), 1, "txn response length is not 1");
                        let range = match txn_res.pop().unwrap() {
                            etcd_client::TxnOpResponse::Range(range) => range,
                            _ => panic!("unexpected txn response"),
                        };
                        let kvs = range.get_kvs();
                        let existed = kvs[0].value_str();
                        let succ_put_value = succ_put_value.lock().await;
                        assert_eq!(&*succ_put_value, existed);
                    }
                }));
            }

            futures::future::join_all(tasks).await;
        });
    }
}
