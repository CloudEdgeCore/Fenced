//! The Wasmtime Fenced runtime provider.
//!
//! This crate hosts untrusted agent components in a capability-free
//! Wasmtime component-model sandbox and drives the fenced Runtime Protocol as
//! a separate process. It forbids unsafe Rust and never links Go code.

pub mod fixture;
pub mod sandbox;
