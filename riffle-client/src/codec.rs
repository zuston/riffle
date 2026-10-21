use std::str::FromStr;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{MAX_MESSAGE_BYTES, RECORD_HEADER_BYTES};

pub(crate) async fn run_codec_task<T: Send + 'static>(
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    // todo: add concurrency limitation
    tokio::task::spawn_blocking(task)
        .await
        .context("shuffle codec task failed")?
}

pub trait Codec: Send + Sync {
    fn encode(&self, block: Bytes) -> Result<Bytes>;
    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes>;
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    #[default]
    Lz4,
    Zstd,
}

impl CompressionCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    pub(crate) fn codec(self) -> &'static dyn Codec {
        match self {
            Self::Lz4 => &Lz4Codec,
            Self::Zstd => &ZstdCodec,
        }
    }
}

impl FromStr for CompressionCodec {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => bail!("unsupported compression codec {value:?}; expected lz4 or zstd"),
        }
    }
}

/// LZ4 blocks use the existing uncompress_length metadata, without a size prefix.
pub struct Lz4Codec;

impl Codec for Lz4Codec {
    fn encode(&self, block: Bytes) -> Result<Bytes> {
        let compressed = lz4_flex::block::compress(&block);
        // Do not retain the compressor's worst-case allocation for a small result.
        Ok(Bytes::from(compressed.into_boxed_slice()))
    }

    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes> {
        ensure!(
            (RECORD_HEADER_BYTES..=MAX_MESSAGE_BYTES).contains(&uncompress_length),
            "invalid uncompressed block length"
        );
        ensure!(
            !block.is_empty() && block.len() <= MAX_MESSAGE_BYTES,
            "invalid compressed block length"
        );
        let mut decoded = vec![0; uncompress_length];
        let length =
            lz4_flex::block::decompress_into(&block, &mut decoded).context("invalid LZ4 block")?;
        ensure!(
            length == uncompress_length,
            "uncompressed block length mismatch"
        );
        Ok(Bytes::from(decoded))
    }
}

/// Zstd frames use the default compression level (3).
pub struct ZstdCodec;

pub const ZSTD_LEVEL: i32 = 3;

impl Codec for ZstdCodec {
    fn encode(&self, block: Bytes) -> Result<Bytes> {
        let compressed =
            zstd::bulk::compress(&block, ZSTD_LEVEL).context("Zstd compression failed")?;
        Ok(Bytes::from(compressed.into_boxed_slice()))
    }

    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes> {
        ensure!(
            (RECORD_HEADER_BYTES..=MAX_MESSAGE_BYTES).contains(&uncompress_length),
            "invalid uncompressed block length"
        );
        ensure!(
            !block.is_empty() && block.len() <= MAX_MESSAGE_BYTES,
            "invalid compressed block length"
        );
        let mut decoded = vec![0; uncompress_length];
        let length =
            zstd::bulk::decompress_to_buffer(&block, &mut decoded).context("invalid Zstd block")?;
        ensure!(
            length == uncompress_length,
            "uncompressed block length mismatch"
        );
        Ok(Bytes::from(decoded))
    }
}
