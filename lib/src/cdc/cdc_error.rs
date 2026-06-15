#![expect(missing_docs)]

use std::sync::mpsc;

use crate::cdc::cdc_config::HASH_LENGTH;

#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error("Missing chunk data for hash {hash:?}")]
    MissingChunk { hash: [u8; HASH_LENGTH] },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Git(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    ChannelSender(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    ChannelReceiver(#[from] mpsc::RecvError),
}

impl CdcError {
    pub fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Git(source.into())
    }

    pub fn from_channel_sender(
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::ChannelSender(source.into())
    }
}

pub type CdcResult<T> = Result<T, CdcError>;
