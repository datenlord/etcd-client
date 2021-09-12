use crate::lease::EtcdLeaseKeepAliveRequest;
use crate::watch::EtcdWatchRequest;
use smol::channel::SendError;

#[derive(thiserror::Error, Debug)]
/// Error type for etcd-client
pub enum EtcdError {
    /// InvalidURI
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    /// Transport
    #[error("gRPC transport error: {0}")]
    Transport(#[from] grpcio::Error),
    /// SendError for ()
    #[error("send error for (): {0}")]
    SendFailed(#[from] SendError<()>),
    /// SendError for EtcdLeaseKeepAliveRequest
    #[error("send error for EtcdLeaseKeepAliveRequest: {0}")]
    SendFailedForLeaseKeepAliveRequest(#[from] SendError<EtcdLeaseKeepAliveRequest>),
    /// SendError for EtcdWatchRequest
    #[error("send error for EtcdWatchRequest: {0}")]
    SendFailedForWatchRequest(#[from] SendError<EtcdWatchRequest>),
    /// Internal Error
    #[error("Internal Error: {0}")]
    InternalError(String),
}
