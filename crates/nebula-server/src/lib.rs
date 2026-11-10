//! NebulaCache server runtime: configuration, lifecycle management, request handling, and telemetry.
//!
//! The executable wires these modules together through [`application::Application`]. The TCP
//! protocol is handled by [`server`], while optional HTTP administration and metrics endpoints
//! are provided by [`admin`] and [`metrics`].

/// HTTP administration API and browser console.
pub mod admin;
/// Application startup, supervision, and graceful shutdown.
pub mod application;
/// Configuration types, defaults, and loading/validation.
pub mod config;
/// Maps decoded cache requests to store operations and protocol responses.
pub mod handler;
/// In-process metrics collection, export, and dashboard snapshots.
pub mod metrics;
/// Snapshot serialization, recovery, and retention for persistent cache mode.
pub mod persistence;
/// TCP listener and protocol connection lifecycle.
pub mod server;
/// systemd notification and watchdog integration.
pub mod systemd;
mod ttl_cleanup;
