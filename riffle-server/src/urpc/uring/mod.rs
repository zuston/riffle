//! io-uring based URPC implementation
//!
//! This module provides io-uring based URPC server and connection implementations
//! for high-performance network communication on Linux.

#[cfg(all(feature = "urpc-io-uring", target_os = "linux"))]
pub mod connection;

#[cfg(all(feature = "urpc-io-uring", target_os = "linux"))]
pub mod server;
