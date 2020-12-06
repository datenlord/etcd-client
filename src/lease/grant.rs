use std::time::Duration;

use crate::protos::rpc::{LeaseGrantRequest, LeaseGrantResponse};
use crate::ResponseHeader;
use utilities::Cast;

/// Request for granting lease.
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
        self.proto.ID = id.cast()
    }
}

impl Into<LeaseGrantRequest> for EtcdLeaseGrantRequest {
    #[inline]
    fn into(self) -> LeaseGrantRequest {
        self.proto
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
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Gets the lease ID for the granted lease.
    #[inline]
    pub fn id(&self) -> u64 {
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
