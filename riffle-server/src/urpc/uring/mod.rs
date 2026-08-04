//! Pluggable io_uring based urpc net engine.
//!
//! This module provides an alternative, completion-driven network stack for
//! the urpc protocol. It is selected via `urpc_config.net_engine = "URING"`
//! and requires the `io-uring` cargo feature on linux.

pub mod bridge;
pub mod encode;
pub mod engine;

pub use bridge::AppCommandBridgeHandler;
pub use engine::{FrameHandler, RemoteResponder, Responder, UringServerConfig, UringUrpcServer};
