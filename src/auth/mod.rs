/// Authenticate mode for Etcd authentication operations.
mod authenticate;

pub use authenticate::{EtcdAuthenticateRequest, EtcdAuthenticateResponse};

use crate::protos::rpc_grpc::AuthClient;
use crate::retryable;
use crate::Result;
use backoff::future::retry;
use backoff::ExponentialBackoff;
use std::time::Duration;
use crate::CURRENT_INTERVAL_VALUE;
use crate::CURRENT_INTERVAL_ENV_KEY;
use crate::INITIAL_INTERVAL_VALUE;
use crate::INITIAL_INTERVAL_ENV_KEY;
use crate::MAX_ELAPSED_TIME_VALUE;
use crate::MAX_ELAPSED_TIME_ENV_KEY;

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
        let authenticate_result = retryable!(|| async {
            let resp = self.client.authenticate_async(&req.clone().into())?;
            Ok(From::from(resp.await?))
        });
        Ok(authenticate_result)
    }
}
