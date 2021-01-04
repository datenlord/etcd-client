/// Authenticate mode for Etcd authentication operations.
mod authenticate;

pub use authenticate::{EtcdAuthenticateRequest, EtcdAuthenticateResponse};

use crate::protos::rpc_grpc::AuthClient;
use crate::Result;

/// Auth client which provides authenticating operation.
#[derive(Clone)]
pub struct Auth {
    /// Etcd Auth client.
    client: AuthClient,
}

impl Auth {
    /// Creates a new Auth client.
    pub(crate) const fn new(client: AuthClient) -> Self {
        Self { client }
    }

    /// Performs an authenticating operation.
    /// It generates an authentication token based on a given user name and password.
    /// # Errors
    /// Will returns `Err` if the status of `response` is not `ok`
    #[inline]
    pub async fn authenticate(
        &mut self,
        req: EtcdAuthenticateRequest,
    ) -> Result<EtcdAuthenticateResponse> {
        let resp = self.client.authenticate_async(&req.into())?;

        Ok(From::from(resp.await?))
    }
}
