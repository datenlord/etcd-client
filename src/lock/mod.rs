/// The mod of lock release operations
mod release;
/// The mod of lock require operations
mod require;

use crate::protos::lock_grpc::LockClient;
use crate::Result as Res;
pub use release::{EtcdUnlockRequest, EtcdUnlockResponse};
pub use require::{EtcdLockRequest, EtcdLockResponse};

/// Lock client.
#[derive(Clone)]
pub struct Lock {
    /// Etcd Lock client.
    client: LockClient,
}

impl Lock {
    /// Creates a new `LockClient`.
    ///
    /// This method should only be called within etcd client.
    pub(crate) const fn new(client: LockClient) -> Self {
        Self { client }
    }

    /// Performs a lock operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn lock(&mut self, req: EtcdLockRequest) -> Res<EtcdLockResponse> {
        let resp = self.client.lock_async(&req.into())?.await?;
        Ok(From::from(resp))
    }

    /// Performs a unlock operation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if RPC call is failed.
    #[inline]
    pub async fn unlock(&mut self, req: EtcdUnlockRequest) -> Res<EtcdUnlockResponse> {
        let resp = self.client.unlock_async(&req.into())?.await?;
        Ok(From::from(resp))
    }
}
