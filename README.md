etcd client for Rust
====

[![CI Status][ci-badge]][ci-url]
[![License][license-badge]][license-url]

[ci-badge]: https://img.shields.io/github/workflow/status/datenlord/etcd-client/CI?style=flat-square
[ci-url]: https://github.com/datenlord/etcd-client/actions
[license-badge]: https://img.shields.io/github/license/datenlord/etcd-client.svg?style=flat-square
[license-url]: https://github.com/datenlord/etcd-client/blob/master/LICENSE

This is an [etcd](https://github.com/etcd-io/etcd)(API v3) client for Rust, which is refactored from [etcd-rs](https://github.com/luncj/etcd-rs) project.

## Road Map
- [x] 0.1 Replace the tokio and tonic library with smol and grpc-rs, which are more lightweight libraries.
- [x] 0.2 Refactor etcd api to use grpc-rs async APIs to provide real async functionalities.
- [x] 0.3 Apply the single key-value lock free Cache to etcd client, which is based on the etcd watch mechanism.
- [x] 0.4 Apply the lru replacement policies with lock to etcd Cache.
- [x] 0.5 Add the retry with exponential backoff mechanism to etcd client.
- [ ] 0.6 Support the Cache recovery mechanism.
- [ ] 0.7 Support the lock free lru replacement policies to etcd Cache.
- [ ] 0.8 Support range key-value Cache.