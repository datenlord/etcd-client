use std::time::Duration;

use crate::protos::rpc::{LeaseGrantRequest, LeaseGrantResponse};
use crate::ResponseHeader;
use clippy_utilities::Cast;

/// Request for granting lease.
#[derive(Debug, Clone)]
pub struct EtcdLeaseGrantRequest {
    /// Etcd lease grant request.
    proto: LeaseGrantRequest,
}

impl EtcdLeaseGrantRequest {
    /// Creates a new `LeaseGrantRequest` with the specified TTL.
    #[inline]
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        let proto = LeaseGrantRequest {
            TTL: ttl.as_secs().cast(),
            ID: 0,
            ..LeaseGrantRequest::default()
        };

        Self { proto }
    }

    /// Set custom lease ID.
    #[inline]
    pub fn set_id(&mut self, id: u64) {
        self.proto.ID = id.cast();
    }
}

impl From<EtcdLeaseGrantRequest> for LeaseGrantRequest {
    #[inline]
    fn from(e: EtcdLeaseGrantRequest) -> Self {
        e.proto
    }
}

#[derive(Debug)]
/// `LeaseGrant` Response
pub struct EtcdLeaseGrantResponse {
    /// Etcd lease grant response.
    proto: LeaseGrantResponse,
}

impl EtcdLeaseGrantResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }

    /// Gets the lease ID for the granted lease.
    #[inline]
    pub fn id(&self) -> u64 {
        log::debug!("lease id: {}", self.proto.ID);
        self.proto.ID.cast()
    }

    /// Gets the server chosen lease time-to-live in seconds.
    #[inline]
    pub fn ttl(&self) -> u64 {
        self.proto.TTL.cast()
    }
}

impl From<LeaseGrantResponse> for EtcdLeaseGrantResponse {
    #[inline]
    fn from(resp: LeaseGrantResponse) -> Self {
        Self { proto: resp }
    }
}
