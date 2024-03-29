use crate::protos::rpc::{AuthenticateRequest, AuthenticateResponse};
use crate::ResponseHeader;

/// Request for authenticating.
#[derive(Debug, Clone)]
pub struct EtcdAuthenticateRequest {
    /// Etcd authenticate request.
    proto: AuthenticateRequest,
}

impl EtcdAuthenticateRequest {
    /// Creates a new `EtcdAuthenticateRequest`.
    #[inline]
    pub fn new<N, P>(name: N, password: P) -> Self
    where
        N: Into<String>,
        P: Into<String>,
    {
        let proto = AuthenticateRequest {
            name: name.into(),
            password: password.into(),
            ..AuthenticateRequest::default()
        };
        Self { proto }
    }
}

impl From<EtcdAuthenticateRequest> for AuthenticateRequest {
    #[inline]
    fn from(e: EtcdAuthenticateRequest) -> Self {
        e.proto
    }
}

/// Response for authenticating.
#[derive(Debug)]
pub struct EtcdAuthenticateResponse {
    /// Etcd authenticate response.
    proto: AuthenticateResponse,
}

impl EtcdAuthenticateResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.proto.header.take().map(From::from)
    }

    /// Gets an authorized token that can be used in succeeding RPCs.
    #[inline]
    pub fn token(&self) -> &str {
        &self.proto.token
    }
}

impl From<AuthenticateResponse> for EtcdAuthenticateResponse {
    #[inline]
    fn from(resp: AuthenticateResponse) -> Self {
        Self { proto: resp }
    }
}
