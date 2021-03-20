use crate::protos::rpc::{LeaseRevokeRequest, LeaseRevokeResponse};
use crate::ResponseHeader;
use utilities::Cast;

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

impl Into<LeaseRevokeRequest> for EtcdLeaseRevokeRequest {
    #[inline]
    fn into(self) -> LeaseRevokeRequest {
        self.proto
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
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }
}

impl From<LeaseRevokeResponse> for EtcdLeaseRevokeResponse {
    #[inline]
    fn from(resp: LeaseRevokeResponse) -> Self {
        Self { proto: resp }
    }
}
