//! An asynchronously etcd client for Rust.
//!
//! etcd-rs supports etcd v3 API and async/await syntax.
//!
//! # Examples
//!
//! A simple key-value read and write operation:
//!
//! ```no_run
//! use etcd_rs::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = Client::connect(ClientConfig {
//!         endpoints: vec!["http://127.0.0.1:2379".to_owned()],
//!         auth: None,
//!         tls: None,
//!     }).await?;
//!
//!     let key = "foo";
//!     let value = "bar";
//!
//!     // Put a key-value pair
//!     let resp = client.kv().put(PutRequest::new(key, value)).await?;
//!
//!     println!("Put Response: {:?}", resp);
//!
//!     // Get the key-value pair
//!     let resp = client
//!         .kv()
//!         .range(RangeRequest::new(KeyRange::key(key)))
//!         .await?;
//!     println!("Range Response: {:?}", resp);
//!
//!     // Delete the key-valeu pair
//!     let resp = client
//!         .kv()
//!         .delete(DeleteRequest::new(KeyRange::key(key)))
//!         .await?;
//!     println!("Delete Response: {:?}", resp);
//!
//!    Ok(())
//! }
//! ```

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
