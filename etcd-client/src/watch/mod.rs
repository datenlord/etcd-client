//! The Watch API provides an event-based interface for asynchronously monitoring changes to keys.
//!
//! # Examples
//!
//! Watch key `foo` changes
//!
//! ```no_run
//!
//! use etcd_client::*;
//! use futures::stream::StreamExt;
//!
//! fn main() -> Result<()> {
//!     smol::block_on(async {
//!     let config = ClientConfig::new(vec!["http://127.0.0.1:2379".to_owned()], None, 32, true);
//!     let client = Client::connect(config).await?;
//!
//!         // print out all received watch responses
//!         let mut inbound = client.watch(KeyRange::key("foo")).await.unwrap();
//!         smol::spawn(async move {
//!             while let Some(resp) = inbound.recv().await {
//!                 println!("watch response: {:?}", resp);
//!             }
//!         });
//!
//!         let key = "foo";
//!         client.kv().put(EtcdPutRequest::new(key, "bar")).await?;
//!         client.kv().put(EtcdPutRequest::new(key, "baz")).await?;
//!         client
//!             .kv()
//!             .delete(EtcdDeleteRequest::new(KeyRange::key(key)))
//!             .await?;
//!
//!         // not necessary, but will cleanly shut down the long-running tasks
//!         // spawned by the client
//!         client.shutdown().await;
//!
//!         Ok(())
//!     })
//! }
//!
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_broadcast::{InactiveReceiver, Receiver as BroadcastRx, Sender as BroadcastTx};
use async_std::channel::bounded;
use crossbeam_queue::SegQueue;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::{SinkExt, TryFutureExt};
use grpcio::{StreamingCallSink, WriteFlags};
use smol::channel::{unbounded, Sender};
use smol::Task;

pub use watch_impl::{EtcdWatchRequest, EtcdWatchResponse};

use crate::lazy::Lazy;
use crate::protos::kv;
use crate::protos::rpc::WatchRequest;
use crate::protos::rpc_grpc::WatchClient;
use crate::EtcdKeyValue;
use crate::KeyRange;
use crate::Result;

/// Watch implementation mod.
mod watch_impl;

/// the timeout of waiting etcd response
const WATCH_REQUEST_TIMEOUT_SEC: u64 = 2;

/// watch id
type WatchID = i64;

/// the recorded watch info in a lock
#[derive(Default)]
struct WatchedMap {
    /// keyrange to watchid
    keyrange_2_watchid: BTreeMap<KeyRange, WatchID>,
    /// watchid to (sender to user, user receiver)
    watchid_2_detail: HashMap<
        WatchID,
        (
            BroadcastTx<Option<EtcdWatchResponse>>,
            Weak<SingleWatchEventReceiverInner>,
        ),
    >,
}
impl WatchedMap {
    /// add new watched info
    fn add_watched(
        &mut self,
        watchid: WatchID,
        keyrange: KeyRange,
        sender_2_user: BroadcastTx<Option<EtcdWatchResponse>>,
        user_receiver: Weak<SingleWatchEventReceiverInner>,
    ) {
        self.keyrange_2_watchid.insert(keyrange, watchid);
        self.watchid_2_detail
            .insert(watchid, (sender_2_user, user_receiver));
    }
    /// remove the watch to cancel
    fn remove_watch(&mut self, keyrange: &KeyRange) -> Option<WatchID> {
        if let Some(watch_id) = self.keyrange_2_watchid.remove(keyrange) {
            log::debug!("removed watch");
            drop(
                self.watchid_2_detail
                    .remove(&watch_id)
                    .unwrap_or_else(|| panic!("")),
            );
            return Some(watch_id);
        }
        None
    }

    /// get sender 2 user to send watched event
    fn get_sender_2_user(
        &self,
        watchid: WatchID,
    ) -> Option<BroadcastTx<Option<EtcdWatchResponse>>> {
        if let Some(&(ref sender, _)) = self.watchid_2_detail.get(&watchid) {
            return Some(sender.clone());
        }
        None
    }

    /// get receiver of key range to send back to user who requested watch
    fn get_arc_receiver(&self, keyrange: &KeyRange) -> Option<SingleWatchEventReceiver> {
        self.keyrange_2_watchid.get(keyrange).and_then(|id|{
            if let Some(&(_,ref weak_receiver_inner)) = self.watchid_2_detail.get(id) {
                match weak_receiver_inner.upgrade() {
                    Some(arc_receiver_inner) => Some(SingleWatchEventReceiver::from_exist_inner(
                        arc_receiver_inner,
                    )),
                    None => {
                        // `remove_watch` in `WatchedMap` should be called when receiver is dropped
                        panic!(
                            "Receivers were all dropped but the registed info has not been removed, which is impossible"
                        );
                    }
                }
            } else {
                None
            }
        })
    }
}

/// Watch related data shared between watch communication task and user receivers
struct WatchTunnelShared {
    /// A map shared to get the sender to registed watches for a keyrange
    watched_map: Lazy<WatchedMap>,
    /// Queued watch requests
    queued_watch_requests: SegQueue<(EtcdWatchRequest, Sender<Option<SingleWatchEventReceiver>>)>,
    /// Watch request waiting for response
    waiting_watch_request: Lazy<Option<(KeyRange, Sender<Option<SingleWatchEventReceiver>>)>>,

    /// A channel sender to send cancel request to etcd
    ///  cancel request: KeyRange to find the watch id and
    cancel_req_sender: Sender<WatchID>,
    /// Waiting cancel requests, bool refers to whether the response arrived in time
    waiting_cancels: Lazy<HashMap<WatchID, Sender<()>>>,

    /// A channel sender to send shutdowm request.
    shutdown: Sender<()>,

    /// Sub tasks
    sub_tasks: Option<SegQueue<Task<()>>>,
}
impl Drop for WatchTunnelShared {
    fn drop(&mut self) {
        let sub_tasks = self
            .sub_tasks
            .take()
            .unwrap_or_else(|| panic!("sub_tasks should be some until dropped"));
        futures::executor::block_on(async {
            while let Some(task) = sub_tasks.pop() {
                task.await;
            }
        });
    }
}
impl WatchTunnelShared {
    /// new `WatchTunnelShared`
    fn new(cancel_req_sender: Sender<WatchID>, shutdown: Sender<()>) -> Self {
        Self {
            watched_map: Lazy::new(WatchedMap::default),
            waiting_cancels: Lazy::new(HashMap::new),
            queued_watch_requests: SegQueue::new(),
            waiting_watch_request: Lazy::new(|| None),
            cancel_req_sender,
            shutdown,
            sub_tasks: Some(SegQueue::new()),
        }
    }

    /// cancel a watch in async task.
    async fn cancel_watch(&self, keyrange: KeyRange) {
        log::debug!("cancel watch {}", keyrange);
        let mut watched_map = self.watched_map.write().await;
        if let Some(watchid) = watched_map.remove_watch(&keyrange) {
            let (tx, rx) = bounded::<()>(1);
            self.waiting_cancels.write().await.insert(watchid, tx);
            if self.cancel_req_sender.send(watchid).await.is_ok() {
                self.sub_tasks
                    .as_ref()
                    .unwrap_or_else(|| { panic!("sub_tasks should be some until dropped") })
                    .push(
                        smol::spawn(async move {
                            futures::select! {
                        _ = smol::Timer::after(Duration::from_secs(WATCH_REQUEST_TIMEOUT_SEC)).into_future().fuse()=>{
                            // todo: add retry for failed request
                            // return Err(EtcdError::WaitingResponseTimeout("waiting for cancel response when calling `cancel_watch`".to_owned()));
                            log::debug!("cancel watch wait response timeout");
                        }
                        res = rx.recv().into_future().fuse()=>{
                            res.unwrap_or_else(|e|{
                                panic!("receive cancel response channel shouldn't be destroyed, err:{e}");
                            });
                            log::debug!("cancel watch successed");
                        }
                    }
                        })
                    );
            }
        } else {
            panic!("logic bug, cancel watch should be called only when there's watched key");
        }
    }
}
/// `WatchTunnel` is a reusable connection for `Watch` operation
/// The underlying `gRPC` method is Bi-directional streaming
#[allow(dead_code)]
struct WatchTunnel {
    /// A channel sender to send watch request to send loop.
    watch_req_sender: Sender<(EtcdWatchRequest, Sender<Option<SingleWatchEventReceiver>>)>,
    /// A channel receiver to receive watch response.
    // resp_receiver: Option<Receiver<Result<EtcdWatchResponse>>>,

    /// Shared
    shared: Arc<WatchTunnelShared>,
}

impl WatchTunnel {
    #[allow(clippy::too_many_lines)]
    /// Creates a new `WatchClient`.
    fn new(client: &WatchClient) -> Self {
        let (watch_req_sender, watch_req_receiver) =
            unbounded::<(EtcdWatchRequest, Sender<Option<SingleWatchEventReceiver>>)>();
        // todo: cancel operation
        let (cancel_req_sender, cancel_req_receiver) = unbounded::<WatchID>();
        // From recv loop to send loop, notify a watch request is done, next watch can be excuted.
        let (waiting_watch_response_tx, waiting_watch_response_rx) = bounded::<()>(1);
        let (shutdown_tx, shutdown_rx) = unbounded::<()>();
        let shutdown_reponse = shutdown_rx.clone();
        // Monitor inbound watch response and transfer to the receiver
        let (mut client_req_sender, mut client_resp_receiver) = client
            .watch()
            .unwrap_or_else(|e| panic!("failed to send watch command, the error is: {}", e));

        let shared = Arc::new(WatchTunnelShared::new(cancel_req_sender, shutdown_tx));
        let shared2 = Arc::clone(&shared);
        let shared3 = Arc::clone(&shared);

        // Send loop
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_rx.into_future().fuse();

            #[allow(clippy::mut_mut)]
            loop {
                /// send watch request or add to queue or get receiver directly
                ///  return true if a request is sent
                async fn handle_watch_request(
                    client_req_sender: &mut StreamingCallSink<WatchRequest>,
                    shared: &WatchTunnelShared,
                    req: EtcdWatchRequest,
                    send_back: Sender<Option<SingleWatchEventReceiver>>
                ) -> bool {
                    let keyrange = KeyRange::range(req.get_key(), req.get_range_end());
                    // The locking operation on the map here is mutually exclusive with the map operation of cancel_watch.
                    //  Therefore, the sender to the user will definitely be valid during the map holding period.
                    let watched_map = shared.watched_map.read().await;
                    if let Some(event_receiver) = watched_map.get_arc_receiver(&keyrange) {
                        log::debug!("{} watched directly return", keyrange);
                        // already watched
                        if let Err(err) = send_back.send(Some(event_receiver)).await {
                            panic!(
                                "Send watch receiver to user failed, Watch canceled, err: {err}"
                            );
                        }
                    } else if shared.waiting_watch_request.read().await.is_some() {
                        log::debug!("watch queued");
                        shared.queued_watch_requests.push((req, send_back));
                    } else {
                        drop(watched_map);
                        log::debug!("{} isn't watched, send new watch request", keyrange);
                        // new watch request
                        *shared.waiting_watch_request.write().await = Some((keyrange, send_back));

                        client_req_sender
                            .send((req.into(), WriteFlags::default()))
                            .fuse().await
                            .unwrap_or_else(|e| panic!("Fail to send request, the error is {}", e));
                        // waiting_watch_response=true;
                        return true;
                    }
                    false
                }

                futures::select! {
                    //1. Wait new watch request
                    res = watch_req_receiver.recv().into_future().fuse() => {
                        // received user
                        if let Ok((req,send_back)) = res {
                            handle_watch_request(&mut client_req_sender,&shared,req,send_back).await;
                        }else{
                            break;
                        }
                    },
                    //2. wait new cancel request
                    // send
                    res = cancel_req_receiver.recv().into_future().fuse() => {
                        if let Ok(watch_id) = res {
                            client_req_sender.send(
                                (EtcdWatchRequest::cancel(watch_id).into(), WriteFlags::default())
                            ).fuse().await.unwrap_or_else(
                                |e| panic!("Fail to send request, the error is {}", e)
                            );
                        }else{
                            break;
                        }
                    },
                    //3. wait watch response
                    // if has queuened request send
                    _ = waiting_watch_response_rx.recv().into_future().fuse() =>{
                        // receive when a watch request got its response
                        // waiting_watch_response=false;
                        while let Some((req,send_back))= shared.queued_watch_requests.pop(){
                            log::debug!("handle queued watch request");
                            if handle_watch_request(&mut client_req_sender,&shared,req,send_back).await{
                                // left request will be handled after current request get it's response
                                break;
                            }
                        }
                    },
                    _ = shutdown_rx => { break; },
                }
            }
        }).detach();
        // Receive loop
        smol::spawn(async move {
            let mut shutdown_rx = shutdown_reponse.into_future().fuse();
            loop {
                #[allow(clippy::mut_mut)]
                let resp =
                    futures::select! {
                    resp_opt = client_resp_receiver.next().fuse() => resp_opt.unwrap_or_else(
                        || panic!("Fail to receive reponse from client")
                    ),
                    _ = shutdown_rx => { return; }
                };

                match resp {
                    Ok(resp) => {
                        // watch create response
                        if resp.created {
                            let (keyrange, send_back) = shared2.waiting_watch_request
                                .write().await
                                .take()
                                .unwrap_or_else(|| {
                                    panic!(
                                        "watch create response must have a waiting create request"
                                    )
                                });

                            let (tx, rx) =
                                async_broadcast::broadcast::<Option<EtcdWatchResponse>>(10);
                            // let (tx,rx)=unbounded::<Option<EtcdWatchResponse>>();
                            let receiver_for_user = SingleWatchEventReceiver::new(
                                Arc::clone(&shared2),
                                rx,
                                keyrange.clone()
                            );
                            shared2.watched_map
                                .write().await
                                .add_watched(
                                    resp.watch_id,
                                    keyrange.clone(),
                                    tx,
                                    receiver_for_user.get_weak_inner()
                                );
                            // send watch result back to user
                            if let Err(e) = send_back.send(Some(receiver_for_user)).await {
                                panic!(
                                    "user receiver shouldn't be dropped before watch response arrive, err:{e}"
                                );
                            }
                            // notify the send loop to handle next watch requests
                            waiting_watch_response_tx
                                .send(()).await
                                .unwrap_or_else(|e| {
                                    panic!(
                                        "Failed to send watch resp from recv loop to send loop, err:{e}"
                                    )
                                });
                            log::debug!(
                                "watch created response received and registed id:{} keyrange:{}",
                                resp.watch_id,
                                keyrange
                            );
                        } else if resp.canceled {
                            let sendback = shared2.waiting_cancels
                                .write().await
                                .remove(&resp.watch_id)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "watch id must be recorded in `waiting_cancels` before receive cancel respinse"
                                    )
                                });
                            sendback
                                .send(()).await
                                .unwrap_or_else(|e| {
                                    panic!("send back channel shouldn't be destroyed, err:{e}")
                                });
                        } else {
                            // The locking operation on the map here is mutually exclusive with the map operation of cancel_watch.
                            //  Therefore, the sender to the user will definitely be valid during the map holding period.
                            let watched_map = shared2.watched_map.read().await;
                            let sendback = watched_map.get_sender_2_user(resp.watch_id);
                            if let Some(sender) = sendback {
                                log::debug!("watch event received and sent");
                                sender.broadcast(Some(resp.into())).await.unwrap_or_else(|e| {
                                    panic!(
                                        "User receiver shouldn't be dropped and send back should work, err:{e}"
                                    );
                                });
                            } else {
                                log::debug!("received watch event but no user to send to");
                            }
                        }
                    }
                    Err(e) => {
                        log::debug!("Watch end with error: {e}");
                        break;
                    }
                }
            }
        }).detach();

        Self {
            watch_req_sender,
            shared: shared3,
        }
    }

    /// shutdown the watch client
    async fn shutdown(&self) -> Result<()> {
        self.shared.shutdown.send(()).await?;
        Ok(())
    }
}

/// shared inner of `SingleWatchEventReceiver`
struct SingleWatchEventReceiverInner {
    /// A receiver to receive etcd watched event
    receiver: InactiveReceiver<Option<EtcdWatchResponse>>,
    /// A tunnel used to communicate with Etcd server for watch operations.
    shared: Arc<WatchTunnelShared>,
    /// Watched keyrange
    keyrange: Option<KeyRange>,
}
impl Drop for SingleWatchEventReceiverInner {
    fn drop(&mut self) {
        // send cancel task to send loop, after sent
        futures::executor::block_on(async {
            self.shared
                .cancel_watch(self.keyrange.take().unwrap_or_else(|| {
                    panic!("keyrange in SingleWatchEventReceiverInner should be some until dropped")
                }))
                .await;
        });
    }
}

/// Watch result return to user
pub struct SingleWatchEventReceiver {
    /// The inner arc, when all `SingleWatchEventReceiver` dropped,
    ///   `SingleWatchEventReceiverInner` fn drop will be triggered.
    ///   In this drop, we should do the cancel watch operation.
    inner: Arc<SingleWatchEventReceiverInner>,

    /// A receiver to receive etcd watched event
    receiver: BroadcastRx<Option<EtcdWatchResponse>>,
}

impl SingleWatchEventReceiver {
    /// weak inner will be stored to create new inner
    ///  if all receivers dropped, weak inner will be invalid
    fn get_weak_inner(&self) -> Weak<SingleWatchEventReceiverInner> {
        Arc::downgrade(&self.inner)
    }

    /// if there's prev watch, an inner will be stored to clone to create new receiver
    fn from_exist_inner(inner: Arc<SingleWatchEventReceiverInner>) -> Self {
        let receiver = inner.receiver.activate_cloned();

        Self { inner, receiver }
    }

    /// new `SingleWatchEventReceiver`
    /// - `shared` A tunnel used to communicate with Etcd server for watch operations.
    /// - `receiver` A receiver to receive etcd watched event
    /// - `keyrange` Watched keyrange
    fn new(
        shared: Arc<WatchTunnelShared>,
        receiver: BroadcastRx<Option<EtcdWatchResponse>>,
        keyrange: KeyRange,
    ) -> Self {
        let inner_receiver = receiver.clone().deactivate();
        Self {
            inner: Arc::new(SingleWatchEventReceiverInner {
                shared,
                keyrange: Some(keyrange),
                receiver: inner_receiver,
            }),
            receiver,
        }
    }

    /// Blocking recv a watch event until system end.
    pub async fn recv(&mut self) -> Option<EtcdWatchResponse> {
        match self.receiver.recv().await {
            Ok(received) => received,
            Err(err) => {
                log::debug!("Receive event channel destroyed, the system is closing. err: {err}");
                None
            }
        }
    }
}

/// Watch client.
#[derive(Clone)]
pub struct Watch {
    /// A tunnel used to communicate with Etcd server for watch operations.
    tunnel: Arc<WatchTunnel>,
}

impl Watch {
    /// Create a new `WatchClient`.
    pub(crate) fn new(client: &WatchClient) -> Self {
        let tunnel = Arc::new(WatchTunnel::new(client));

        Self { tunnel }
    }

    /// Performs a watch operation.
    /// Will fail if
    ///
    /// # Panics
    ///
    /// Will panic if send watch request error.
    #[inline]
    pub async fn watch(&mut self, key_range: KeyRange) -> Option<SingleWatchEventReceiver> {
        let (tx, rx) = unbounded();
        if let Err(err) = self
            .tunnel
            .watch_req_sender
            .send((EtcdWatchRequest::create(key_range), tx))
            .await
        {
            log::debug!(
                "send watch watch request failed, the channel is destroyed and the system is closed, err:{err}"
            );
            return None;
        }

        rx.recv()
            .await
            .unwrap_or_else(|e| panic!("watch resp channel shouldn't be ineffective, err:{}", e))
    }

    /// Shut down the running watch task, if any.
    ///
    /// # Errors
    ///
    /// Will return `Err` if tunnel is shutdown.
    #[inline]
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.

        self.tunnel.shutdown().await?;
        Ok(())
    }
}

/// The kind of event.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Put event.
    Put,

    /// Delete event.
    Delete,
}

impl From<kv::Event_EventType> for EventType {
    #[inline]
    fn from(event_type: kv::Event_EventType) -> Self {
        match event_type {
            kv::Event_EventType::PUT => Self::Put,
            kv::Event_EventType::DELETE => Self::Delete,
        }
    }
}

/// Every change to every key is represented with Event messages.
#[derive(Debug)]
pub struct Event {
    /// Etcd event proto.
    proto: kv::Event,
}

impl Event {
    /// Takes the key-value pair out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_kvs(&mut self) -> Option<EtcdKeyValue> {
        self.proto.kv.take().map(From::from)
    }

    /// Get the type of event
    #[inline]
    pub fn event_type(&self) -> EventType {
        EventType::from(self.proto.get_field_type())
    }
}

impl From<kv::Event> for Event {
    #[inline]
    fn from(event: kv::Event) -> Self {
        Self { proto: event }
    }
}
