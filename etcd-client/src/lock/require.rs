use crate::protos::lock::{LockRequest, LockResponse};
use crate::ResponseHeader;
use utilities::Cast;

/// Request for requiring a lock
pub struct EtcdLockRequest {
    /// Etcd lock request
    proto: LockRequest,
}

impl EtcdLockRequest {
    /// Creates a new `EtcdLockRequest` for requiring a lock
    #[inline]
    pub fn new<T>(name: T, lease: u64) -> Self
    where
        T: Into<Vec<u8>>,
    {
        let lock_request = LockRequest {
            name: name.into(),
            lease: lease.cast(),
            ..LockRequest::default()
        };

        Self {
            proto: lock_request,
        }
    }

    /// Get the name from `LockRequest`
    #[inline]
    pub fn get_name(&self) -> Vec<u8> {
        self.proto.get_name().to_vec()
    }

    /// Get the name from `LockRequest`
    #[inline]
    pub fn get_lease(&self) -> u64 {
        self.proto.get_lease().cast()
    }
}

impl From<EtcdLockRequest> for LockRequest {
    #[inline]
    fn from(e: EtcdLockRequest) -> Self {
        e.proto
    }
}

/// Response for requring a lock.
#[derive(Debug)]
pub struct EtcdLockResponse {
    /// Etcd lock response
    proto: LockResponse,
}

impl EtcdLockResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }

    /// Take the key out of response, leaving a empty Vec in its place.
    #[inline]
    pub fn take_key(&mut self) -> Vec<u8> {
        self.proto.take_key()
    }
}

impl From<LockResponse> for EtcdLockResponse {
    #[inline]
    fn from(resp: LockResponse) -> Self {
        Self { proto: resp }
    }
}
