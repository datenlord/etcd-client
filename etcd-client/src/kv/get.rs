use super::EtcdKeyValue;
use crate::protos::rpc::{
    RangeRequest, RangeRequest_SortOrder, RangeRequest_SortTarget, RangeResponse,
};
use crate::ResponseHeader;
use clippy_utilities::Cast;
use protobuf::RepeatedField;

/// Request for fetching a single key-value pair.
#[derive(Debug, Clone)]
pub struct EtcdGetRequest {
    /// Etcd range fetching request.
    proto: RangeRequest,
}

impl EtcdGetRequest {
    /// Creates a new `RangeRequest` for a specified key.
    #[inline]
    #[must_use]
    pub fn new<K>(key: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        let range_request = RangeRequest {
            key: key.into(),
            range_end: vec![],
            limit: 0,
            revision: 0,
            sort_order: RangeRequest_SortOrder::NONE,
            sort_target: RangeRequest_SortTarget::KEY,
            serializable: false,
            keys_only: false,
            count_only: false,
            min_mod_revision: 0,
            max_mod_revision: 0,
            min_create_revision: 0,
            max_create_revision: 0,
            ..RangeRequest::default()
        };
        Self {
            proto: range_request,
        }
    }

    /// Sets the maximum number of keys returned for the request.
    /// When limit is set to 0, it is treated as no limit.
    #[inline]
    pub fn set_limit(&mut self, limit: usize) {
        self.proto.limit = limit.cast();
    }

    /// Gets the `key_range` from the `RangeRequest`.
    #[inline]
    pub fn get_key(&self) -> &[u8] {
        self.proto.get_key()
    }
}

impl From<EtcdGetRequest> for RangeRequest {
    #[inline]
    fn from(e: EtcdGetRequest) -> Self {
        e.proto
    }
}

/// Response for `RangeRequest`.
#[derive(Debug)]
pub struct EtcdGetResponse {
    /// Etcd range fetching response.
    proto: RangeResponse,
}

impl EtcdGetResponse {
    /// Creates a new `EtcdGetResponse` for a specified key.
    #[inline]
    pub const fn new(range_response: RangeResponse) -> Self {
        Self {
            proto: range_response,
        }
    }

    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }

    /// Takes the key-value pairs out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_kvs(&mut self) -> Vec<EtcdKeyValue> {
        let kvs = std::mem::replace(&mut self.proto.kvs, RepeatedField::from_vec(vec![]));

        kvs.into_iter().map(From::from).collect()
    }

    /// Returns `true` if there are more keys to return in the requested range, and `false` otherwise.
    #[inline]
    pub const fn has_more(&self) -> bool {
        self.proto.more
    }

    /// Returns the number of keys within the range when requested.
    #[inline]
    pub fn count(&self) -> usize {
        self.proto.count.cast()
    }

    /// Gets the key-value pairs from the response.
    #[inline]
    pub fn get_kvs(&self) -> Vec<EtcdKeyValue> {
        self.proto.kvs.clone().into_iter().map(From::from).collect()
    }

    /// Consume `EtcdGetRequest` and return inner `RangeResponse`
    #[allow(clippy::missing_const_for_fn)] // false alarm
    #[inline]
    pub fn get_inner(self) -> RangeResponse {
        self.proto
    }
}

impl From<RangeResponse> for EtcdGetResponse {
    #[inline]
    fn from(resp: RangeResponse) -> Self {
        Self { proto: resp }
    }
}
