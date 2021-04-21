use crate::lease::EtcdLeaseKeepAliveRequest;
use smol::channel::SendError;

#[derive(thiserror::Error, Debug)]
/// Error type for etcd-client
pub enum EtcdError {
    /// InvalidURI
    #[error("invalid URI")]
    InvalidURI(#[from] http::uri::InvalidUri),
    /// Transport
    #[error("gRPC transport error")]
    Transport(#[from] grpcio::Error),
    /// SendError for ()
    #[error("send error for ()")]
    SendFailed(#[from] SendError<()>),
    /// SendError for EtcdLeaseKeepAliveRequest
    #[error("send error for EtcdLeaseKeepAliveRequest")]
    SendFailedForLeaseKeepAliveRequest(#[from] SendError<EtcdLeaseKeepAliveRequest>),
    /// Internal Error
    #[error("Internal Error: {0}")]
    InternalError(String),
}
