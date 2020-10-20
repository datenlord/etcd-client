//! An asynchronously etcd client for Rust.
//!
//! etcd-client supports etcd v3 API and async/await syntax.

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    elided_lifetimes_in_paths, // allow anonymous lifetime
    // missing_copy_implementations,
    // missing_debug_implementations,
    // missing_docs, // TODO: add documents
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
    // variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]
// #![deny(
//     clippy::clone_on_ref_ptr,
//     clippy::dbg_macro,
//     clippy::enum_glob_use,
//     clippy::get_unwrap,
//     clippy::macro_use_imports
// )]
// #[allow(
//     clippy::suspicious_op_assign_impl,
//     clippy::suspicious_arithmetic_impl,
//     clippy::module_inception
// )]

pub use auth::{Auth, AuthenticateRequest, AuthenticateResponse};
pub use client::{Client, ClientConfig};
pub use error::Error;
pub use kv::{
    DeleteRequest, DeleteResponse, KeyRange, KeyValue, Kv, PutRequest, PutResponse, RangeRequest,
    RangeResponse, TxnCmp, TxnOpResponse, TxnRequest, TxnResponse,
};
pub use lease::{
    Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse,
};
pub use response_header::ResponseHeader;
pub use watch::{Event, EventType, Watch, WatchRequest, WatchResponse};

mod auth;
mod client;
mod error;
mod kv;
mod lazy;
mod lease;
mod proto;
mod response_header;
mod watch;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use async_compat::Compat;
    use std::collections::HashMap;

    const DEFAULT_ETCD_ENDPOINT_FOR_TEST: &str = "http://127.0.0.1:2379";

    #[test]
    fn test_all() -> anyhow::Result<()> {
        smol::block_on(Compat::new(async {
            test_kv().await.context("test etcd kv operations")?;
            Ok::<(), anyhow::Error>(())
        }))?;
        Ok(())
    }

    async fn test_kv() -> anyhow::Result<()> {
        let client = build_etcd_client().await?;
        test_list_prefix(&client).await?;
        Ok(())
    }

    async fn test_list_prefix(client: &Client) -> anyhow::Result<()> {
        let prefix = "42_";
        // Add test data to etcd
        let mut test_data = HashMap::new();
        test_data.insert("41_foo1", "baz1");
        test_data.insert("42_foo1", "baz1");
        test_data.insert("42_foo2", "baz2");
        test_data.insert("42_bar1", "baz3");
        test_data.insert("42_bar2", "baz4");

        for (key, value) in test_data.clone() {
            client.kv().put(PutRequest::new(key, value)).await?;
        }

        // List key-value pairs with prefix
        let req = RangeRequest::new(KeyRange::prefix(prefix));
        let mut resp = client.kv().range(req).await?;

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

        // Delete key-valeu pairs with prefix
        let req = DeleteRequest::new(KeyRange::prefix(prefix));
        let resp = client.kv().delete(req).await?;
        println!("Delete Response: {:?}", resp);

        Ok(())
    }

    async fn build_etcd_client() -> anyhow::Result<Client> {
        let client = Client::connect(ClientConfig {
            endpoints: vec![DEFAULT_ETCD_ENDPOINT_FOR_TEST.to_owned()],
            auth: None,
            tls: None,
        })
        .await?;
        Ok(client)
    }
}
