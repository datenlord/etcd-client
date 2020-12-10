use super::{KeyRange, KeyValue};
use crate::protos::rpc::{
    RangeRequest, RangeRequest_SortOrder, RangeRequest_SortTarget, RangeResponse,
};
use crate::ResponseHeader;
use protobuf::RepeatedField;
use utilities::Cast;

/// Request for fetching key-value pairs.
pub struct EtcdRangeRequest {
    /// Etcd range fetching request.
    proto: RangeRequest,
}

impl EtcdRangeRequest {
    /// Creates a new `RangeRequest` for the specified key range.
    #[inline]
    #[must_use]
    pub fn new(key_range: KeyRange) -> Self {
        let range_request = RangeRequest {
            key: key_range.key,
            range_end: key_range.range_end,
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
}

impl Into<RangeRequest> for EtcdRangeRequest {
    #[inline]
    fn into(self) -> RangeRequest {
        self.proto
    }
}

/// Response for `RangeRequest`.
#[derive(Debug)]
pub struct EtcdRangeResponse {
    /// Etcd range fetching response.
    proto: RangeResponse,
}

impl EtcdRangeResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Takes the key-value pairs out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_kvs(&mut self) -> Vec<KeyValue> {
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
}

impl From<RangeResponse> for EtcdRangeResponse {
    #[inline]
    fn from(resp: RangeResponse) -> Self {
        Self { proto: resp }
    }
}
