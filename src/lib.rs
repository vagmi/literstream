//! literstream — a library-only Rust port of Litestream.
//!
//! Phase 0 delivers the LTX (Lite Transaction) serializer. The [`ltx`] module
//! is byte-compatible with `github.com/superfly/ltx` v3, the format Litestream
//! uses for replication. See `plans/implementation.md` for the roadmap.

pub mod ltx;
pub mod wal;
