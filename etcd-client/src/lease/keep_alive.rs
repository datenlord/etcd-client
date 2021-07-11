use crate::protos::rpc::{LeaseKeepAliveRequest, LeaseKeepAliveResponse};
use crate::ResponseHeader;
use utilities::Cast;

/// Request for refreshing lease.
#[derive(Debug)]
pub struct EtcdLeaseKeepAliveRequest {
    /// Etcd lease keep alive request.
    proto: LeaseKeepAliveRequest,
}

impl EtcdLeaseKeepAliveRequest {
    /// Creates a new `LeaseKeepAliveRequest` which will refresh the specified lease.
    #[inline]
    #[must_use]
    pub fn new(id: u64) -> Self {
        let proto = LeaseKeepAliveRequest {
            ID: id.cast(),
            ..LeaseKeepAliveRequest::default()
        };

        Self { proto }
    }
}

impl Into<LeaseKeepAliveRequest> for EtcdLeaseKeepAliveRequest {
    #[inline]
    fn into(self) -> LeaseKeepAliveRequest {
        self.proto
    }
}

#[derive(Debug)]
/// Response for refreshing lease.
pub struct EtcdLeaseKeepAliveResponse {
    /// Etcd lease keep alive reponse.
    proto: LeaseKeepAliveResponse,
}

impl EtcdLeaseKeepAliveResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Gets the lease ID for the refreshed lease.
    #[inline]
    pub fn id(&self) -> u64 {
        self.proto.ID.cast()
    }

    /// Get the new TTL for the lease.
    #[inline]
    pub fn ttl(&self) -> u64 {
        self.proto.TTL.cast()
    }
}

impl From<LeaseKeepAliveResponse> for EtcdLeaseKeepAliveResponse {
    #[inline]
    fn from(resp: LeaseKeepAliveResponse) -> Self {
        Self { proto: resp }
    }
}
