use crate::protos::lock::{UnlockRequest, UnlockResponse};
use crate::ResponseHeader;

/// Request for requiring a lock
pub struct EtcdUnlockRequest {
    /// Etcd lock request
    proto: UnlockRequest,
}

impl EtcdUnlockRequest {
    /// Creates a new `EtcdUnlockRequest` for requiring a lock
    #[inline]
    pub fn new<T>(key: T) -> Self
    where
        T: Into<Vec<u8>>,
    {
        let lock_request = UnlockRequest {
            key: key.into(),
            ..UnlockRequest::default()
        };

        Self {
            proto: lock_request,
        }
    }

    /// Get the name from `UnlockRequest`
    #[inline]
    pub fn get_key(&self) -> Vec<u8> {
        self.proto.get_key().to_vec()
    }
}

impl From<EtcdUnlockRequest> for UnlockRequest {
    #[inline]
    fn from(e: EtcdUnlockRequest) -> Self {
        e.proto
    }
}

/// Response for requring a lock.
#[derive(Debug)]
pub struct EtcdUnlockResponse {
    /// Etcd lock response
    proto: UnlockResponse,
}

impl EtcdUnlockResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }
}

impl From<UnlockResponse> for EtcdUnlockResponse {
    #[inline]
    fn from(resp: UnlockResponse) -> Self {
        Self { proto: resp }
    }
}
