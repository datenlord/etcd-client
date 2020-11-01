use crate::protos::rpc::{PutRequest, PutResponse};
use crate::KeyValue;
use crate::ResponseHeader;
use utilities::Cast;

/// Request for putting key-value.
pub struct EtcdPutRequest {
    /// Etcd put key-value pairs request.
    proto: PutRequest,
}

impl EtcdPutRequest {
    /// Creates a new `EtcdPutRequest` for saving the specified key-value.
    #[inline]
    pub fn new<K, V>(key: K, value: V) -> Self
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let put_request = PutRequest {
            key: key.into(),
            value: value.into(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
            ..PutRequest::default()
        };
        Self { proto: put_request }
    }

    /// Sets the lease ID to associate with the key in the key-value store.
    /// A lease value of 0 indicates no lease.
    #[inline]
    pub fn set_lease(&mut self, lease: u64) {
        self.proto.lease = lease.cast();
    }

    /// When set, responds with the key-value pair data before the update from this Put request.
    #[inline]
    pub fn set_prev_kv(&mut self, prev_kv: bool) {
        self.proto.prev_kv = prev_kv;
    }

    /// When set, update the key without changing its current value. Returns an error if the key does not exist.
    #[inline]
    pub fn set_ignore_value(&mut self, ignore_value: bool) {
        self.proto.ignore_value = ignore_value;
    }

    /// When set, update the key without changing its current lease. Returns an error if the key does not exist.
    #[inline]
    pub fn set_ignore_lease(&mut self, ignore_lease: bool) {
        self.proto.ignore_lease = ignore_lease;
    }
}

impl Into<PutRequest> for EtcdPutRequest {
    #[inline]
    fn into(self) -> PutRequest {
        self.proto
    }
}

/// Response for putting key-value.
#[derive(Debug)]
pub struct EtcdPutResponse {
    /// Etcd put key-value pairs response.
    proto: PutResponse,
}

impl EtcdPutResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Takes the previous key-value pair out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_prev_kv(&mut self) -> Option<KeyValue> {
        match self.proto.prev_kv.take() {
            Some(kv) => Some(From::from(kv)),
            None => None,
        }
    }
}

impl From<PutResponse> for EtcdPutResponse {
    #[inline]
    fn from(resp: PutResponse) -> Self {
        Self { proto: resp }
    }
}
