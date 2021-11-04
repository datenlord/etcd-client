use super::{EtcdKeyValue, KeyRange};
use crate::protos::rpc::{DeleteRangeRequest, DeleteRangeResponse};
use crate::ResponseHeader;
use protobuf::RepeatedField;
use utilities::Cast;

/// Request for deleting key-value pairs.
#[derive(Debug, Clone)]
pub struct EtcdDeleteRequest {
    /// Etcd delete range key-value pairs request.
    proto: DeleteRangeRequest,
}

impl EtcdDeleteRequest {
    /// Creates a new `EtcdDeleteRequest` for the specified key range.
    #[inline]
    #[must_use]
    pub fn new(key_range: KeyRange) -> Self {
        let delete_range_request = DeleteRangeRequest {
            key: key_range.key,
            range_end: key_range.range_end,
            prev_kv: false,
            ..DeleteRangeRequest::default()
        };
        Self {
            proto: delete_range_request,
        }
    }

    /// When set, responds with the key-value pair data before the update from this Delete request.
    #[inline]
    pub fn set_prev_kv(&mut self, prev_kv: bool) {
        self.proto.prev_kv = prev_kv;
    }
}

impl From<EtcdDeleteRequest> for DeleteRangeRequest {
    #[inline]
    fn from(e: EtcdDeleteRequest) -> Self {
        e.proto
    }
}

/// Response for `DeleteRequest`.
#[derive(Debug)]
pub struct EtcdDeleteResponse {
    /// Etcd delete range key-value pairs response.
    proto: DeleteRangeResponse,
}

impl EtcdDeleteResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }

    /// Returns the number of keys deleted by the delete range request.
    #[inline]
    pub fn count_deleted(&self) -> usize {
        self.proto.deleted.cast()
    }

    /// Takes the previous key-value pairs out of response, leaving an empty vector in its place.
    #[inline]
    pub fn take_prev_kvs(&mut self) -> Vec<EtcdKeyValue> {
        let kvs = std::mem::replace(&mut self.proto.prev_kvs, RepeatedField::from_vec(vec![]));

        kvs.into_iter().map(From::from).collect()
    }

    /// Returns `true` if the previous key-value pairs is not empty, and `false` otherwise.
    #[inline]
    pub fn has_prev_kvs(&self) -> bool {
        !self.proto.prev_kvs.is_empty()
    }

    /// Gets the previous kvs from `DeleteRangeResponse`.
    #[inline]
    pub fn get_prev_kvs(&self) -> Vec<EtcdKeyValue> {
        self.proto
            .prev_kvs
            .clone()
            .into_iter()
            .map(From::from)
            .collect()
    }
}

impl From<DeleteRangeResponse> for EtcdDeleteResponse {
    #[inline]
    fn from(resp: DeleteRangeResponse) -> Self {
        Self { proto: resp }
    }
}
