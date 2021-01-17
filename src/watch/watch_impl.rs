use crate::protos::rpc::{
    WatchCancelRequest, WatchCreateRequest, WatchRequest, WatchRequest_oneof_request_union,
    WatchResponse,
};
use crate::Event;
use crate::KeyRange;
use crate::ResponseHeader;
use utilities::Cast;

/// Request for creating or canceling watch.
#[derive(Debug, Clone)]
pub struct EtcdWatchRequest {
    /// Etcd watch key request.
    proto: WatchRequest,
}

impl EtcdWatchRequest {
    /// Creates a new `WatchRequest` which will subscribe events of the specified key.
    #[inline]
    #[must_use]
    pub fn create(mut key_range: KeyRange) -> Self {
        let watch_create_request = WatchCreateRequest {
            key: key_range.take_key(),
            range_end: key_range.take_range_end(),
            start_revision: 0,
            progress_notify: false,
            filters: vec![],
            prev_kv: false,
            ..WatchCreateRequest::default()
        };

        let mut watch_request = WatchRequest::new();
        watch_request.set_create_request(watch_create_request);

        Self {
            proto: watch_request,
        }
    }

    /// Creates a new `WatchRequest` which will unsubscribe the specified watch.
    #[inline]
    #[must_use]
    pub fn cancel(watch_id: usize) -> Self {
        let mut watch_cancel_request = WatchCancelRequest::new();
        watch_cancel_request.set_watch_id(watch_id.cast());
        let mut watch_request = WatchRequest::new();
        watch_request.set_cancel_request(watch_cancel_request);
        Self {
            proto: watch_request,
        }
    }

    /// Sets the revision to watch from (inclusive). No `start_revision` is "now".
    /// It only effects when the request is for subscribing.
    #[inline]
    pub fn set_start_revision(&mut self, revision: usize) {
        // TODO log warning if not CreateRequest
        if let Some(&mut WatchRequest_oneof_request_union::create_request(ref mut req)) =
            self.proto.request_union.as_mut()
        {
            req.start_revision = revision.cast()
        }
    }

    /// Sets progress notify.
    /// It only effects when the request is for subscribing.
    #[inline]
    pub fn set_progress_notify(&mut self, progress_notify: bool) {
        // TODO log warning if not CreateRequest
        if let Some(&mut WatchRequest_oneof_request_union::create_request(ref mut req)) =
            self.proto.request_union.as_mut()
        {
            req.progress_notify = progress_notify
        }
    }

    /// Sets previous key value.
    /// It only effects when the request is for subscribing.
    #[inline]
    pub fn set_prev_kv(&mut self, prev_kv: bool) {
        // TODO log warning if not CreateRequest
        if let Some(&mut WatchRequest_oneof_request_union::create_request(ref mut req)) =
            self.proto.request_union.as_mut()
        {
            req.prev_kv = prev_kv
        }
    }

    /// Gets the watch key.
    /// It only effects when the request is for subscribing.
    #[inline]
    pub fn get_key(&self) -> Vec<u8> {
        if let Some(WatchRequest_oneof_request_union::create_request(req)) =
            self.proto.request_union.clone()
        {
            return req.get_key().to_vec();
        }
        vec![]
    }

    /// Returns if the watch request is a create watch request.
    #[inline]
    pub fn is_create(&self) -> bool {
        if self.proto.has_create_request() {
            return true;
        }
        false
    }
}

impl Into<WatchRequest> for EtcdWatchRequest {
    #[inline]
    fn into(self) -> WatchRequest {
        self.proto
    }
}

#[derive(Debug)]
/// Watch Response
pub struct EtcdWatchResponse {
    /// Etcd watch key response.
    proto: WatchResponse,
}

impl EtcdWatchResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Gets the ID of the watcher that corresponds to the response.
    #[inline]
    pub fn watch_id(&self) -> u64 {
        self.proto.watch_id.cast()
    }

    /// Takes the events out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_events(&mut self) -> Vec<Event> {
        let events = std::mem::take(&mut self.proto.events);

        events.into_iter().map(From::from).collect()
    }
}

impl From<WatchResponse> for EtcdWatchResponse {
    #[inline]
    fn from(resp: WatchResponse) -> Self {
        Self { proto: resp }
    }
}
