use crate::protos::rpc;
use clippy_utilities::Cast;

/// Response header.
#[derive(Debug)]
pub struct ResponseHeader {
    /// Etcd response header which includes cluster metadata for all responses from etcd API.
    proto: rpc::ResponseHeader,
}

impl ResponseHeader {
    /// Get the key-value store revision when the request was applied.
    #[inline]
    pub fn revision(&self) -> u64 {
        self.proto.revision.cast()
    }
}

impl From<rpc::ResponseHeader> for ResponseHeader {
    #[inline]
    fn from(header: rpc::ResponseHeader) -> Self {
        Self { proto: header }
    }
}
