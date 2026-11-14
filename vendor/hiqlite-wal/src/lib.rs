// Copyright 2026 Sebastian Dobe <sebastiandobe@mailbox.org>

#![doc = include_str!("../README.md")]

pub use crate::writer::LogSync;
pub use log_store::{LogStore, LogStoreReader};
pub use shutdown::ShutdownHandle;
pub use status::{
    BoundedWalError, WalRecoveryObservation, WalRuntimeState, WalStatusHandle, WalStatusSnapshot,
};
pub use writer::Action;

pub mod error;
pub mod inspection;
mod lockfile;
mod log_store;
mod log_store_impl;
mod metadata;
mod reader;
mod shutdown;
mod status;
mod utils;
mod wal;
#[cfg(feature = "migration")]
pub mod writer;
#[cfg(not(feature = "migration"))]
mod writer;

#[cfg(feature = "validation-test-helpers")]
#[doc(hidden)]
pub mod codec_compatibility;
