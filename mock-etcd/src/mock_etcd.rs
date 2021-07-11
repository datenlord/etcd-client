//! The implementation for Mock Etcd

use super::etcd::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse, WatchRequest,
    WatchResponse,
};
use super::etcd_grpc::{create_kv, create_lease, create_watch, Kv, Lease, Watch};
use super::kv::{Event, Event_EventType, KeyValue};
use super::lock::{LockRequest, LockResponse, UnlockRequest, UnlockResponse};
use super::lock_grpc::{create_lock, Lock};
use async_io::Timer;
use async_lock::RwLock;
use futures::future::TryFutureExt;
use futures::prelude::*;
use grpcio::{
    DuplexSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
    ServerBuilder, UnarySink, WriteFlags,
};
use log::{debug, error};
use protobuf::RepeatedField;
use smol::lock::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use utilities::Cast;

/// Help function to send success `gRPC` response
async fn success<R: Send>(response: R, sink: UnarySink<R>) {
    sink.success(response)
        .map_err(|e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ())
        .await
}

/// Send failure `gRPC` response
fn fail<R>(ctx: &RpcContext, sink: UnarySink<R>, rsc: RpcStatusCode, details: String) {
    debug_assert_ne!(
        rsc,
        RpcStatusCode::OK,
        "the input RpcStatusCode should not be OK"
    );
    let rs = RpcStatus::new(rsc, Some(details));
    let f = sink
        .fail(rs)
        .map_err(|e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
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
struct KeyRange {
    /// The first key of the range and should be non-empty
    key: Vec<u8>,
    /// The key following the last key of the range
    range_end: Vec<u8>,
}

/// Mock Etcd
#[derive(Clone)]
struct MockEtcd {
    /// map to store key value
    map: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,

    /// map to store key and watch ids
    watch: Arc<RwLock<HashMap<i64, KeyRange>>>,

    /// map to store watch id and watch response senders
    watch_response_sender: Arc<RwLock<HashMap<i64, LockedDuplexSink>>>,

    /// set to store lock name
    lock_map: Arc<RwLock<HashSet<Vec<u8>>>>,
}

/// A locked `DuplexSink` for watch response
type LockedDuplexSink = Arc<Mutex<DuplexSink<WatchResponse>>>;

/// Sequence increasing watch id
static WATCH_ID_COUNTER: AtomicI64 = AtomicI64::new(0);

/// Range end to get all keys
const ALL_KEYS: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

impl MockEtcd {
    /// Create `MockEtcd`
    fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            watch: Arc::new(RwLock::new(HashMap::new())),
            watch_response_sender: Arc::new(RwLock::new(HashMap::new())),
            lock_map: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Get values of keys from a `RangeRequest` to map
    #[allow(clippy::pattern_type_mismatch)]
    async fn map_get(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        req: RangeRequest,
    ) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut kvs = vec![];
        let map = map_arc.read().await;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = map.get(&key) {
                    kvs.push(kv.clone());
                }
            }
            ALL_KEYS => {
                if key == vec![0_u8] {
                    map.values().for_each(|v| kvs.push(v.clone()));
                }
            }
            _ => {
                map.iter().for_each(|(k, v)| {
                    if k >= &key && k < &range_end {
                        kvs.push(v.clone())
                    }
                });
            }
        }
        kvs
    }

    /// Send watch response for a specific watch id to etcd client
    async fn send_watch_response_with_watch_id(
        sender: LockedDuplexSink,
        kv: KeyValue,
        prev_kv: Option<KeyValue>,
        watch_id: i64,
        event_type: Event_EventType,
    ) {
        let mut event = Event::new();
        event.set_field_type(event_type);
        if let Some(value) = prev_kv {
            event.set_prev_kv(value);
        }
        event.set_kv(kv);

        let mut response = WatchResponse::new();
        response.set_watch_id(watch_id);
        response.set_events(RepeatedField::from_vec([event].to_vec()));

        sender
            .lock()
            .await
            .send((response, WriteFlags::default()))
            .await
            .unwrap_or_else(|e| panic!("Fail to send watch response, the error is {}", e));
    }

    /// Send watch response to etcd client
    #[allow(clippy::pattern_type_mismatch)]
    async fn send_watch_responses(
        watch_arc: Arc<RwLock<HashMap<i64, KeyRange>>>,
        watch_response_hashmap_arc: Arc<RwLock<HashMap<i64, LockedDuplexSink>>>,
        kv: KeyValue,
        prev_kv: Option<KeyValue>,
        event_type: Event_EventType,
    ) {
        // Find all watch ids which watch this key and send watch response
        let watch = watch_arc.read().await;
        let watch_response_sender = watch_response_hashmap_arc.read().await;
        for (watch_id, v) in watch.iter() {
            let sender = watch_response_sender
                .get(watch_id)
                .unwrap_or_else(|| panic!("Fail to get watch response sender from map"));
            match v.range_end.as_slice() {
                ONE_KEY => {
                    if v.key == kv.get_key() {
                        Self::send_watch_response_with_watch_id(
                            Arc::clone(sender),
                            kv.clone(),
                            prev_kv.clone(),
                            *watch_id,
                            event_type,
                        )
                        .await;
                    }
                }
                ALL_KEYS => {
                    if v.key == vec![0_u8] {
                        Self::send_watch_response_with_watch_id(
                            Arc::clone(sender),
                            kv.clone(),
                            prev_kv.clone(),
                            *watch_id,
                            event_type,
                        )
                        .await;
                    }
                }
                _ => {
                    if kv.get_key().to_vec() >= v.key && kv.get_key().to_vec() < v.range_end {
                        Self::send_watch_response_with_watch_id(
                            Arc::clone(sender),
                            kv.clone(),
                            prev_kv.clone(),
                            *watch_id,
                            event_type,
                        )
                        .await;
                    }
                }
            }
        }
    }

    /// Insert a key value from a `PutRequest` to map
    async fn map_insert(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        watch_arc: Arc<RwLock<HashMap<i64, KeyRange>>>,
        watch_response_hashmap_arc: Arc<RwLock<HashMap<i64, LockedDuplexSink>>>,
        req: PutRequest,
    ) -> Option<KeyValue> {
        let mut kv = KeyValue::new();
        kv.set_key(req.get_key().to_vec());
        kv.set_value(req.get_value().to_vec());
        let mut map = map_arc.write().await;
        let prev_kv = map.get(&req.get_key().to_vec()).cloned();
        let insert_res = map.insert(req.get_key().to_vec(), kv.clone());
        Self::send_watch_responses(
            watch_arc,
            watch_response_hashmap_arc,
            kv.clone(),
            prev_kv,
            Event_EventType::PUT,
        )
        .await;
        insert_res
    }

    /// Delete keys from `DeleteRangeRequest` from map
    #[allow(clippy::pattern_type_mismatch)]
    async fn map_delete(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        watch_arc: Arc<RwLock<HashMap<i64, KeyRange>>>,
        watch_response_hashmap_arc: Arc<RwLock<HashMap<i64, LockedDuplexSink>>>,
        req: DeleteRangeRequest,
    ) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut prev_kvs = vec![];
        let mut map = map_arc.write().await;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = map.remove(&key) {
                    prev_kvs.push(kv);
                }
            }
            ALL_KEYS => {
                if key == vec![0_u8] {
                    map.values().for_each(|v| prev_kvs.push(v.clone()));
                    map.clear()
                }
            }
            _ => {
                map.retain(|k, v| {
                    if k >= &key && k < &range_end {
                        prev_kvs.push(v.clone());
                        false
                    } else {
                        true
                    }
                });
            }
        }
        for kv in prev_kvs.clone() {
            Self::send_watch_responses(
                Arc::clone(&watch_arc),
                Arc::clone(&watch_response_hashmap_arc),
                kv.clone(),
                Some(kv.clone()),
                Event_EventType::DELETE,
            )
            .await;
        }
        prev_kvs
    }
}

impl Watch for MockEtcd {
    fn watch(
        &mut self,
        _ctx: RpcContext,
        mut stream: RequestStream<WatchRequest>,
        sink: DuplexSink<WatchResponse>,
    ) {
        let watch_arc = Arc::clone(&self.watch);
        let watch_response_sender_arc = Arc::clone(&self.watch_response_sender);
        let task = async move {
            let sink_arc = Arc::new(Mutex::new(sink));
            while let Some(request) = stream.next().await {
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
                            let watch_id = WATCH_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
                            watch_arc.write().await.insert(watch_id, key_range);
                            watch_response_sender_arc
                                .write()
                                .await
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
                        } else {
                            let watch_id = watch_request.get_cancel_request().get_watch_id();
                            watch_arc.write().await.remove(&watch_id);
                            watch_response_sender_arc.write().await.remove(&watch_id);

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
                        error!("Fail to receive watch request, the error is: {}", e);
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
        let map_arc = Arc::clone(&self.map);
        let task = async move {
            let kvs = Self::map_get(map_arc, req).await;
            let mut response = RangeResponse::new();
            response.set_count(kvs.len().cast());
            response.set_kvs(RepeatedField::from_vec(kvs));
            success(response, sink).await;
        };

        smol::spawn(task).detach();
    }

    fn put(&mut self, _ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        debug!(
            "Receive put request key={:?}, value={:?}",
            req.get_key(),
            req.get_value()
        );

        let map_arc = Arc::clone(&self.map);
        let watch_arc = Arc::clone(&self.watch);
        let watch_response_sender_arc = Arc::clone(&self.watch_response_sender);
        let task = async move {
            let mut response = PutResponse::new();
            let prev = Self::map_insert(map_arc, watch_arc, watch_response_sender_arc, req).await;
            if let Some(kv) = prev {
                response.set_prev_kv(kv);
            }
            success(response, sink).await;
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

        let map_arc = Arc::clone(&self.map);
        let watch_arc = Arc::clone(&self.watch);
        let watch_response_sender_arc = Arc::clone(&self.watch_response_sender);
        let task = async move {
            let mut response = DeleteRangeResponse::new();
            let get_prev = req.get_prev_kv();
            let prev_kvs =
                Self::map_delete(map_arc, watch_arc, watch_response_sender_arc, req).await;
            response.set_deleted(prev_kvs.len().cast());
            if get_prev {
                response.set_prev_kvs(RepeatedField::from_vec(prev_kvs));
            }
            success(response, sink).await;
        };
        smol::spawn(task).detach();
    }

    fn txn(&mut self, ctx: RpcContext, _req: TxnRequest, sink: UnarySink<TxnResponse>) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_owned(),
        )
    }

    fn compact(
        &mut self,
        ctx: RpcContext,
        _req: CompactionRequest,
        sink: UnarySink<CompactionResponse>,
    ) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_owned(),
        )
    }
}

impl Lock for MockEtcd {
    fn lock(&mut self, _ctx: RpcContext, req: LockRequest, sink: UnarySink<LockResponse>) {
        debug!("Receive lock request key={:?}", req.get_name(),);
        let map_arc = Arc::clone(&self.lock_map);
        let task = async move {
            loop {
                let mut map = map_arc.write().await;
                if map.contains(req.get_name()) {
                    Timer::after(Duration::from_secs(1)).await;
                    drop(map);
                } else {
                    map.insert(req.get_name().to_vec());
                    drop(map);
                    let mut response = LockResponse::new();
                    response.set_key(req.get_name().to_vec());
                    success(response, sink).await;
                    break;
                }
            }
        };

        smol::spawn(task).detach();
    }

    fn unlock(&mut self, _ctx: RpcContext, req: UnlockRequest, sink: UnarySink<UnlockResponse>) {
        debug!("Receive unlock request key={:?}", req.get_key(),);
        let map_arc = Arc::clone(&self.lock_map);
        let task = async move {
            let mut map = map_arc.write().await;
            if map.contains(req.get_key()) {
                map.remove(req.get_key());
            } else {
            }
            drop(map);
            let response = UnlockResponse::new();
            success(response, sink).await;
        };

        smol::spawn(task).detach();
    }
}

impl Lease for MockEtcd {
    fn lease_grant(
        &mut self,
        _ctx: RpcContext,
        _req: LeaseGrantRequest,
        sink: UnarySink<LeaseGrantResponse>,
    ) {
        let task = async move {
            let mut response = LeaseGrantResponse::new();
            response.set_ID(1);
            success(response, sink).await;
        };

        smol::spawn(task).detach();
    }

    fn lease_revoke(
        &mut self,
        ctx: RpcContext,
        _req: LeaseRevokeRequest,
        sink: UnarySink<LeaseRevokeResponse>,
    ) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_owned(),
        )
    }

    fn lease_keep_alive(
        &mut self,
        ctx: RpcContext,
        _req: RequestStream<LeaseKeepAliveRequest>,
        sink: DuplexSink<LeaseKeepAliveResponse>,
    ) {
        let rs = RpcStatus::new(
            RpcStatusCode::UNIMPLEMENTED,
            Some("Not Implemented".to_owned()),
        );
        let f = sink
            .fail(rs)
            .map_err(|e| error!("failed to send response, the error is: {:?}", e))
            .map(|_| ());
        ctx.spawn(f)
    }

    fn lease_time_to_live(
        &mut self,
        ctx: RpcContext,
        _req: LeaseTimeToLiveRequest,
        sink: UnarySink<LeaseTimeToLiveResponse>,
    ) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_owned(),
        )
    }
}

#[cfg(test)]
#[allow(clippy::all, clippy::restriction)]
#[allow(clippy::too_many_lines)]
mod test {
    use crate::mock_etcd::{MockEtcd, MockEtcdServer};
    use etcd_client::{
        Client, ClientConfig, EtcdDeleteRequest, EtcdLeaseGrantRequest, EtcdLockRequest,
        EtcdPutRequest, EtcdRangeRequest, EtcdUnlockRequest, KeyRange,
    };
    use futures::StreamExt;
    use std::sync::Arc;
    #[test]
    fn test_all() {
        unit_test();
        e2e_test();
        e2e_watch_test();
        e2e_lock_lease_test();
    }
    fn unit_test() {
        smol::future::block_on(async {
            let mock_etcd = MockEtcd::new();
            // Test insert
            let mut put000 = crate::etcd::PutRequest::new();
            let mut put001 = crate::etcd::PutRequest::new();
            let mut put010 = crate::etcd::PutRequest::new();
            let mut put011 = crate::etcd::PutRequest::new();
            let mut put100 = crate::etcd::PutRequest::new();
            let mut put101 = crate::etcd::PutRequest::new();
            let mut put110 = crate::etcd::PutRequest::new();
            let mut put111 = crate::etcd::PutRequest::new();
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

            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put000.clone()
                )
                .await,
                None
            );

            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put001.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put010.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put011.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put100.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put101.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put110.clone()
                )
                .await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    put111.clone()
                )
                .await,
                None
            );
            assert_eq!(
                {
                    let kv = MockEtcd::map_insert(
                        Arc::clone(&mock_etcd.map),
                        Arc::clone(&mock_etcd.watch),
                        Arc::clone(&mock_etcd.watch_response_sender),
                        put000.clone(),
                    )
                    .await;
                    kv.unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            // Test get
            // get one key
            let mut one_key_1 = crate::etcd::RangeRequest::new();
            one_key_1.set_key(vec![0_u8]);
            one_key_1.set_range_end(vec![]);
            let mut one_key_2 = crate::etcd::RangeRequest::new();
            one_key_2.set_key(vec![0_u8, 0_u8, 0_u8]);
            one_key_2.set_range_end(vec![]);
            // get all keys
            let mut all_keys = crate::etcd::RangeRequest::new();
            all_keys.set_key(vec![0_u8]);
            all_keys.set_range_end(vec![0_u8]);
            // get range
            let mut range1 = crate::etcd::RangeRequest::new();
            range1.set_key(vec![0_u8, 0_u8, 0_u8]);
            range1.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            let mut range2 = crate::etcd::RangeRequest::new();
            range2.set_key(vec![0_u8, 0_u8, 0_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);
            let mut range2 = crate::etcd::RangeRequest::new();
            range2.set_key(vec![0_u8, 1_u8, 1_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);

            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), one_key_1.clone()).await,
                vec![]
            );
            assert_eq!(
                {
                    let kv = MockEtcd::map_get(Arc::clone(&mock_etcd.map), one_key_2.clone()).await;
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), all_keys.clone())
                    .await
                    .len(),
                8
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), range1.clone())
                    .await
                    .len(),
                2
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), range2.clone())
                    .await
                    .len(),
                4
            );

            // Test delete
            let mut delete_no_exist = crate::etcd::DeleteRangeRequest::new();
            delete_no_exist.set_key(vec![0_u8]);
            delete_no_exist.set_range_end(vec![]);

            let mut delete_one_key = crate::etcd::DeleteRangeRequest::new();
            delete_one_key.set_key(vec![1_u8, 1_u8, 1_u8]);
            delete_one_key.set_range_end(vec![]);
            // delete range
            let mut delete_range = crate::etcd::DeleteRangeRequest::new();
            delete_range.set_key(vec![0_u8, 0_u8, 0_u8]);
            delete_range.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            // delete all
            let mut delete_all = crate::etcd::DeleteRangeRequest::new();
            delete_all.set_key(vec![0_u8]);
            delete_all.set_range_end(vec![0_u8]);

            assert_eq!(
                MockEtcd::map_delete(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    delete_no_exist.clone()
                )
                .await
                .len(),
                0
            );
            assert_eq!(
                {
                    let kv = MockEtcd::map_delete(
                        Arc::clone(&mock_etcd.map),
                        Arc::clone(&mock_etcd.watch),
                        Arc::clone(&mock_etcd.watch_response_sender),
                        delete_one_key.clone(),
                    )
                    .await;
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![1_u8, 1_u8, 1_u8]
            );
            assert_eq!(mock_etcd.map.read().await.len(), 7);
            assert_eq!(
                MockEtcd::map_delete(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    delete_range.clone()
                )
                .await
                .len(),
                2
            );
            assert_eq!(mock_etcd.map.read().await.len(), 5);
            assert_eq!(
                MockEtcd::map_delete(
                    Arc::clone(&mock_etcd.map),
                    Arc::clone(&mock_etcd.watch),
                    Arc::clone(&mock_etcd.watch_response_sender),
                    delete_all.clone()
                )
                .await
                .len(),
                5
            );
            assert_eq!(mock_etcd.map.read().await.len(), 0);
        });
    }

    fn e2e_test() {
        let mut etcd_server = MockEtcdServer::new();
        etcd_server.start();

        smol::future::block_on(async {
            let endpoints = vec!["127.0.0.1:2379".to_owned()];
            let client = Client::connect(ClientConfig {
                endpoints,
                auth: None,
                cache_enable: false,
                cache_size: 0,
            })
            .await
            .unwrap_or_else(|err| {
                panic!("failed to connect to etcd server, the error is: {}", err)
            });

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

    fn e2e_watch_test() {
        let mut etcd_server = MockEtcdServer::new();
        etcd_server.start();

        smol::future::block_on(async {
            let endpoints = vec!["127.0.0.1:2379".to_owned()];
            let client = Client::connect(ClientConfig {
                endpoints,
                auth: None,
                cache_enable: false,
                cache_size: 0,
            })
            .await
            .unwrap_or_else(|err| {
                panic!("failed to connect to etcd server, the error is: {}", err)
            });

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
                .await;

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

            if let Some(resp) = resp_receiver.next().await {
                assert_eq!(
                    resp.unwrap_or_else(|e| panic!(
                        "Fail to get watch response, the error is {}",
                        e
                    ))
                    .watch_id(),
                    0
                );
            }
            for _x in 0..3 {
                if let Some(resp) = resp_receiver.next().await {
                    let mut watch_resp = resp.unwrap_or_else(|e| {
                        panic!("Fail to get watch response, the error is {}", e)
                    });
                    assert_eq!(watch_resp.watch_id(), 0);
                    let mut events = watch_resp.take_events();
                    let event = events
                        .get_mut(0)
                        .unwrap_or_else(|| panic!("Fail to take events from watch response"));
                    let kv = event
                        .take_kvs()
                        .unwrap_or_else(|| panic!("Fail to take kvs from watch response"))
                        .take_key()
                        .to_vec();
                    assert!((kv == key000.clone()) || (kv == key001.clone()));
                }
            }
        });
    }

    fn e2e_lock_lease_test() {
        let mut etcd_server = MockEtcdServer::new();
        etcd_server.start();

        smol::future::block_on(async {
            let endpoints = vec!["127.0.0.1:2379".to_owned()];
            let client = Client::connect(ClientConfig {
                endpoints,
                auth: None,
                cache_enable: false,
                cache_size: 0,
            })
            .await
            .unwrap_or_else(|err| {
                panic!("failed to connect to etcd server, the error is: {}", err)
            });

            let lock_key012 = vec![0_u8, 1_u8, 2_u8];

            let mut res = client
                .lock()
                .lock(EtcdLockRequest::new(lock_key012.clone(), 10))
                .await
                .unwrap_or_else(|err| panic!("failed to lock key012, the error is {}", err));
            assert_eq!(res.take_key(), lock_key012);
            client
                .lock()
                .unlock(EtcdUnlockRequest::new(lock_key012.clone()))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
            let mut res = client
                .lock()
                .lock(EtcdLockRequest::new(lock_key012.clone(), 10))
                .await
                .unwrap_or_else(|err| panic!("failed to lock key012, the error is {}", err));
            assert_eq!(res.take_key(), lock_key012);
            client
                .lock()
                .unlock(EtcdUnlockRequest::new(lock_key012.clone()))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));

            let res = client
                .lease()
                .grant(EtcdLeaseGrantRequest::new(std::time::Duration::from_secs(
                    10,
                )))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
            assert_eq!(res.id(), 1);
        });
    }
}
