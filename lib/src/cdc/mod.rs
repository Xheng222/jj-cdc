//! CDC (Change Data Capture) module

/// CDC backend wrapper module
/// This module provides a wrapper around the CDC backend.
pub mod backend_wrapper;

/// CDC error module
pub mod cdc_error;

/// Manifest module
pub mod pointer;

/// Utils module
pub mod utils;

mod cdc_config;
mod chunk_backend;
mod manifest_backend;
mod store_backend;
