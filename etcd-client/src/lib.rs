//! An asynchronously etcd client for Rust.
//!
//! etcd-client supports etcd v3 API and async/await syntax.
//!
//! # Examples
//!
//! A simple key-value read and write operation:
//!
//! ```no_run
//! use etcd_client::*;
//!
//! fn main() -> Result<()> {
//!     smol::block_on(async {
//!     let client = Client::connect(ClientConfig {
//!         endpoints: vec!["http://127.0.0.1:2379".to_owned()],
//!         auth: None,
//!         cache_size: 32,
//!         cache_enable: true,
//!     }).await?;
//!
//!     let key = "foo";
//!     let value = "bar";
//!
//!     // Put a key-value pair
//!     let resp = client.kv().put(EtcdPutRequest::new(key, value)).await?;
//!
//!     println!("Put Response: {:?}", resp);
//!
//!     // Get the key-value pair
//!     let resp = client
//!         .kv()
//!         .range(EtcdRangeRequest::new(KeyRange::key(key)))
//!         .await?;
//!     println!("Range Response: {:?}", resp);
//!
//!     // Delete the key-valeu pair
//!     let resp = client
//!         .kv()
//!         .delete(EtcdDeleteRequest::new(KeyRange::key(key)))
//!         .await?;
//!     println!("Delete Response: {:?}", resp);
//!
//!     Ok(())
//!     })
//! }
//! ```

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    missing_copy_implementations,
    // missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results,
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow denying clippy::restriction directly
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::unknown_clippy_lints,  // allow rustc and clippy verison mismatch
)]

pub use auth::{Auth, EtcdAuthenticateRequest, EtcdAuthenticateResponse};
pub use client::{Client, ClientConfig};
pub use error::EtcdError;
pub use kv::{
    EtcdDeleteRequest, EtcdDeleteResponse, EtcdKeyValue, EtcdPutRequest, EtcdPutResponse,
    EtcdRangeRequest, EtcdRangeResponse, EtcdTxnRequest, EtcdTxnResponse, KeyRange, Kv, TxnCmp,
    TxnOpResponse,
};
pub use lease::{
    EtcdLeaseGrantRequest, EtcdLeaseGrantResponse, EtcdLeaseKeepAliveRequest,
    EtcdLeaseKeepAliveResponse, EtcdLeaseRevokeRequest, EtcdLeaseRevokeResponse, Lease,
};
pub use lock::Lock;
pub use lock::{EtcdLockRequest, EtcdLockResponse, EtcdUnlockRequest, EtcdUnlockResponse};
pub use response_header::ResponseHeader;
pub use utilities::OverflowArithmetic;
pub use watch::{EtcdWatchRequest, EtcdWatchResponse, Event, EventType, Watch};

use backoff::{future::Sleeper, Notify};
use std::{future::Future, pin::Pin, time::Duration};

/// Auth mod for authentication operations.
mod auth;
/// Client mod for Etcd client operations.
mod client;
/// Error mod for Etcd client error.
mod error;
/// Kv mod for key-value pairs operations.
mod kv;
/// Lazy mod for Etcd client lazy operations.
mod lazy;
/// Lease mod for lease operations.
mod lease;
/// Lock mod for lock operations.
mod lock;
/// Etcd client request and response protos
mod protos;
/// Etcd API response header
mod response_header;
/// Watch mod for watch operations.
mod watch;

/// Result with error information
pub type Result<T> = std::result::Result<T, EtcdError>;

/// The default value for current interval value
pub const CURRENT_INTERVAL_VALUE: u64 = 1;
/// The key of the default value for current interval environment variable
pub const CURRENT_INTERVAL_ENV_KEY: &str = "CURRENT_INTERVAL";
/// The default value for current initial value
pub const INITIAL_INTERVAL_VALUE: u64 = 1;
/// The key of the default value for initial interval environment variable
pub const INITIAL_INTERVAL_ENV_KEY: &str = "INITIAL_INTERVAL";
/// The default value for max elapsed value
pub const MAX_ELAPSED_TIME_VALUE: u64 = 10;
/// The key of the default value for max elapsed environment variable
pub const MAX_ELAPSED_TIME_ENV_KEY: &str = "MAX_ELAPSED_TIME";

/// A retry macro to immediately attempt a function call after failure
#[macro_export]
macro_rules! retryable {
    ($args:expr) => {
        backoff::future::Retry::new(
            crate::SmolSleeper,
            ExponentialBackoff {
                current_interval: Duration::from_secs(
                    match std::env::var(CURRENT_INTERVAL_ENV_KEY) {
                        Ok(val) => val.parse().unwrap(),
                        Err(_) => CURRENT_INTERVAL_VALUE,
                    },
                ),
                initial_interval: Duration::from_secs(
                    match std::env::var(INITIAL_INTERVAL_ENV_KEY) {
                        Ok(val) => val.parse().unwrap(),
                        Err(_) => INITIAL_INTERVAL_VALUE,
                    },
                ),
                max_elapsed_time: Some(Duration::from_secs(
                    match std::env::var(MAX_ELAPSED_TIME_ENV_KEY) {
                        Ok(val) => val.parse().unwrap(),
                        Err(_) => MAX_ELAPSED_TIME_VALUE,
                    },
                )),
                ..ExponentialBackoff::default()
            },
            crate::NoopNotify,
            $args,
        )
        .await?
    };
}

/// The notifier does nothing
#[derive(Debug, Clone, Copy)]
pub struct NoopNotify;

impl<E> Notify<E> for NoopNotify {
    #[inline]
    fn notify(&mut self, _: E, _: Duration) {}
}

/// The smol sleeper wrapper
struct SmolSleeper;

impl Sleeper for SmolSleeper {
    type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
    fn sleep(&self, dur: Duration) -> Self::Sleep {
        Box::pin(sleep(dur))
    }
}

/// A wrapper for smol sleep
async fn sleep(d: Duration) {
    smol::Timer::after(d).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compat::Compat;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::time::Duration;
    use std::time::SystemTime;
    use utilities::Cast;

    const DEFAULT_ETCD_ENDPOINT1_FOR_TEST: &str = "127.0.0.1:2379";
    // Should not connect 2380 port, which will cause lock operation error.
    //const DEFAULT_ETCD_ENDPOINT2_FOR_TEST: &str = "127.0.0.1:2380";

    #[test]
    fn test_all() -> Result<()> {
        smol::block_on(Compat::new(async {
            test_kv().await?;
            test_transaction().await?;
            test_lock().await?;
            test_watch().await?;
            Ok(())
        }))
    }

    async fn test_watch() -> Result<()> {
        let client = build_etcd_client().await?;

        client
            .kv()
            .put(EtcdPutRequest::new("41_foo1", "baz1"))
            .await?;
        client
            .kv()
            .put(EtcdPutRequest::new("42_foo1", "baz2"))
            .await?;

        let watch_key = "41_foo1";
        let mut response = client.watch(KeyRange::key(watch_key)).await;
        assert_eq!(
            response
                .next()
                .await
                .unwrap_or_else(|| panic!("Fail to receive reponse from client"))
                .unwrap_or_else(|e| { panic!("failed to get watch response, the error is: {}", e) })
                .watch_id(),
            0,
            "Receive wrong watch response from etcd server"
        );

        client
            .kv()
            .put(EtcdPutRequest::new("41_foo1", "baz3"))
            .await?;
        let mut watch_response = response
            .next()
            .await
            .unwrap_or_else(|| panic!("Fail to receive reponse from client"))
            .unwrap_or_else(|e| panic!("failed to get watch response, the error is: {}", e));

        assert_eq!(
            watch_response.watch_id(),
            0,
            "Receive wrong watch response from etcd server"
        );
        assert_eq!(
            watch_response.take_events().len(),
            1,
            "Receive wrong watch events from etcd server"
        );
        client.shutdown().await?;
        Ok(())
    }

    async fn test_lock() -> Result<()> {
        // 1. Lock on "ABC"
        let client = build_etcd_client().await?;
        let lease_id = client
            .lease()
            .grant(EtcdLeaseGrantRequest::new(Duration::from_secs(10)))
            .await?
            .id();
        let lease_id_2 = client
            .lease()
            .grant(EtcdLeaseGrantRequest::new(Duration::from_secs(10)))
            .await?
            .id();
        let key_bytes = client
            .lock()
            .lock(EtcdLockRequest::new(b"ABC".to_vec(), lease_id))
            .await?
            .take_key();

        // 2. Wait until the first lock released automatically
        let time1 = SystemTime::now();
        let key_bytes2 = client
            .lock()
            .lock(EtcdLockRequest::new(b"ABC".to_vec(), lease_id_2))
            .await?
            .take_key();
        let time2 = SystemTime::now();

        // wait a least 5 seconds (the first lock has a 10s lease)
        assert!(
            time2
                .duration_since(time1)
                .unwrap_or_else(|e| panic!("Fail to convert time, error is {}", e))
                .as_secs()
                > 5
        );

        let key_slice = key_bytes.as_slice();
        assert_eq!(
            key_slice
                .get(..3)
                .unwrap_or_else(|| panic!("key slice get first 3 bytes failed")),
            b"ABC".to_vec()
        );

        // 3. Release all locks
        client
            .lock()
            .unlock(EtcdUnlockRequest::new(key_bytes))
            .await?;

        client
            .lock()
            .unlock(EtcdUnlockRequest::new(key_bytes2))
            .await?;
        Ok(())
    }

    async fn test_transaction() -> Result<()> {
        let client = build_etcd_client().await?;
        test_compose(&client).await?;
        Ok(())
    }

    async fn test_compose(client: &Client) -> Result<()> {
        let revision;
        {
            let mut resp = client.kv().put(EtcdPutRequest::new("foo", "bar")).await?;
            revision = resp
                .take_header()
                .unwrap_or_else(|| panic!("Fail to take header from response"))
                .revision();

            for v in 0..10 {
                let _ = client
                    .kv()
                    .put(EtcdPutRequest::new(format!("key-{}", v), format!("{}", v)))
                    .await?;
            }
        }

        let txn = EtcdTxnRequest::new()
            .when_value(KeyRange::key("foo"), TxnCmp::Equal, "bar")
            .when_mod_revision(KeyRange::key("foo"), TxnCmp::Equal, revision.cast())
            .and_then(EtcdPutRequest::new("foo", "bar"))
            .and_then(EtcdRangeRequest::new(KeyRange::all()))
            .and_then(EtcdDeleteRequest::new(KeyRange::all()))
            .and_then(EtcdTxnRequest::new())
            .or_else(EtcdPutRequest::new("bar", "baz"));

        let mut txn_resp = client.kv().txn(txn).await?;

        for op_resp in txn_resp.take_responses() {
            match op_resp {
                TxnOpResponse::Put(_resp) => {}
                TxnOpResponse::Range(_resp) => {}
                TxnOpResponse::Delete(resp) => {
                    assert_eq!(
                        resp.count_deleted(),
                        11,
                        "Deleted wrong value from etcd server"
                    );
                }
                TxnOpResponse::Txn(resp) => {
                    assert!(resp.is_success(), "Txn did not success from etcd server");
                }
            }
        }

        // The failure operation should not be proccessed.
        let req = EtcdRangeRequest::new(KeyRange::key("bar"));
        let range_resp = client.kv().range(req).await?;
        assert_eq!(
            range_resp.count(),
            0,
            "The number of data fetched from etcd is wrong",
        );

        Ok(())
    }

    async fn test_kv() -> Result<()> {
        let client = build_etcd_client().await?;
        test_list_prefix(&client).await?;
        test_range_query(&client).await?;
        Ok(())
    }

    async fn test_range_query(client: &Client) -> Result<()> {
        let query_key = "41_foo1";
        // Add test data to etcd
        let mut test_data = HashMap::new();
        test_data.insert("41_foo1", "baz1");
        test_data.insert("42_foo1", "baz1");
        test_data.insert("42_foo2", "baz2");
        test_data.insert("42_bar1", "baz3");
        test_data.insert("42_bar2", "baz4");

        for (key, value) in test_data.clone() {
            client.kv().put(EtcdPutRequest::new(key, value)).await?;
        }

        let req = EtcdRangeRequest::new(KeyRange::key(query_key));
        let range_resp = client.kv().range(req).await?;
        assert_eq!(
            range_resp.count(),
            1,
            "The number of data fetched from etcd is wrong",
        );

        client
            .kv()
            .put(EtcdPutRequest::new(query_key, "newbaz1"))
            .await?;
        let req2 = EtcdRangeRequest::new(KeyRange::key(query_key));
        let mut range_resp2 = client.kv().range(req2).await?;
        assert_eq!(
            range_resp2.count(),
            1,
            "The number of data fetched from etcd is wrong",
        );
        let expect_value: Vec<u8> = "newbaz1".into();
        assert_eq!(
            range_resp2
                .take_kvs()
                .get(0)
                .unwrap_or_else(|| panic!("Fail to get key value from RangeResponse"))
                .value(),
            expect_value,
            "The value of updated data fetched from etcd is wrong",
        );

        // Delete key-valeu pairs with prefix
        let req = EtcdDeleteRequest::new(KeyRange::all());
        let delete_resp = client.kv().delete(req).await?;
        assert_eq!(
            delete_resp.count_deleted(),
            5,
            "The number of data deleted in etcd is wrong",
        );

        // After delete all, query one key should return nothing.
        let req = EtcdRangeRequest::new(KeyRange::key("41_foo1"));
        let range_resp = client.kv().range(req).await?;
        assert_eq!(
            range_resp.count(),
            0,
            "The number of data fetched from etcd is wrong",
        );

        Ok(())
    }

    async fn test_list_prefix(client: &Client) -> Result<()> {
        let prefix = "42_";
        // Add test data to etcd
        let mut test_data = HashMap::new();
        test_data.insert("41_foo1", "newbaz1");
        test_data.insert("42_foo1", "newbaz1");
        test_data.insert("42_foo2", "newbaz2");
        test_data.insert("42_bar1", "newbaz3");
        test_data.insert("42_bar2", "newbaz4");

        for (key, value) in test_data.clone() {
            client.kv().put(EtcdPutRequest::new(key, value)).await?;
        }

        let req = EtcdRangeRequest::new(KeyRange::key("41_foo1"));
        let range_resp = client.kv().range(req).await?;
        assert_eq!(
            range_resp.count(),
            1,
            "The number of data fetched from etcd is wrong",
        );

        // List key-value pairs with prefix
        let req = EtcdRangeRequest::new(KeyRange::prefix(prefix));
        let mut resp = client.kv().range(req).await?;
        assert_eq!(
            resp.count(),
            4,
            "The number of data fetched from etcd is wrong",
        );
        for kv in resp.take_kvs() {
            assert!(
                test_data.contains_key(kv.key_str()),
                "Data fetched from etcd should not exist",
            );
            assert_eq!(
                test_data.get(kv.key_str()),
                Some(&kv.value_str()),
                "Fetched wrong value from etcd server"
            );
        }

        let prefix2 = "41_";
        let req2 = EtcdRangeRequest::new(KeyRange::prefix(prefix2));
        let resp2 = client.kv().range(req2).await?;
        assert_eq!(
            resp2.count(),
            1,
            "The number of data fetched from etcd is wrong",
        );

        // Delete key-valeu pairs with prefix
        let req = EtcdDeleteRequest::new(KeyRange::all());
        let delete_resp = client.kv().delete(req).await?;
        assert_eq!(
            delete_resp.count_deleted(),
            5,
            "The number of data deleted in etcd is wrong",
        );

        // After delete all, query one key should return nothing.
        let req = EtcdRangeRequest::new(KeyRange::key("41_foo1"));
        let range_resp = client.kv().range(req).await?;
        assert_eq!(
            range_resp.count(),
            0,
            "The number of data fetched from etcd is wrong",
        );

        Ok(())
    }

    async fn build_etcd_client() -> Result<Client> {
        let client = Client::connect(ClientConfig {
            endpoints: vec![
                DEFAULT_ETCD_ENDPOINT1_FOR_TEST.to_owned(),
                //DEFAULT_ETCD_ENDPOINT2_FOR_TEST.to_owned(),
            ],
            auth: None,
            cache_size: 64,
            cache_enable: true,
        })
        .await?;
        Ok(client)
    }
}
