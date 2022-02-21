use crate::protos::rpc::{LeaseRevokeRequest, LeaseRevokeResponse};
use crate::ResponseHeader;
use clippy_utilities::Cast;

/// Request for revoking lease.
#[derive(Debug, Clone)]
pub struct EtcdLeaseRevokeRequest {
    /// Etcd lease revoke request.
    proto: LeaseRevokeRequest,
}

impl EtcdLeaseRevokeRequest {
    /// Creates a new `LeaseRevokeRequest` which will revoke the specified lease.
    #[inline]
    #[must_use]
    pub fn new(id: u64) -> Self {
        let proto = LeaseRevokeRequest {
            ID: id.cast(),
            ..LeaseRevokeRequest::default()
        };

        Self { proto }
    }
}

impl From<EtcdLeaseRevokeRequest> for LeaseRevokeRequest {
    #[inline]
    fn from(e: EtcdLeaseRevokeRequest) -> Self {
        e.proto
    }
}

/// Response for revoking lease.
#[derive(Debug)]
pub struct EtcdLeaseRevokeResponse {
    /// Etcd lease revoke response.
    proto: LeaseRevokeResponse,
}

impl EtcdLeaseRevokeResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }
}

impl From<LeaseRevokeResponse> for EtcdLeaseRevokeResponse {
    #[inline]
    fn from(resp: LeaseRevokeResponse) -> Self {
        Self { proto: resp }
    }
}
