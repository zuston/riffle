use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::{MAX_MESSAGE_BYTES, RECORD_HEADER_BYTES};

// todo: use the customized option to control the parallelism of codec tasks, instead of using the default available parallelism.
static CODEC_PERMITS: Lazy<Arc<Semaphore>> = Lazy::new(|| {
    let parallelism = std::thread::available_parallelism().map_or(1, |n| n.get());
    Arc::new(Semaphore::new(parallelism.saturating_sub(1).max(1)))
});

pub(crate) async fn run_codec_task<T: Send + 'static>(
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let permit = CODEC_PERMITS.clone().acquire_owned().await?;
    tokio::task::spawn_blocking(move || {
        // A running blocking task cannot be aborted; retain its permit and input until it exits.
        let _permit = permit;
        task()
    })
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

pub struct NoOpCodec;

impl Codec for NoOpCodec {
    fn encode(&self, block: Bytes) -> Result<Bytes> {
        Ok(block)
    }

    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes> {
        ensure!(block.len() == uncompress_length, "block length mismatch");
        Ok(block)
    }
}

/// LZ4 blocks use the existing uncompress_length metadata, without a size prefix.
/// Equal stored and uncompressed lengths identify raw blocks, including older writes.
pub struct Lz4Codec;

impl Codec for Lz4Codec {
    fn encode(&self, block: Bytes) -> Result<Bytes> {
        let compressed = lz4_flex::block::compress(&block);
        if compressed.len() >= block.len() {
            return NoOpCodec.encode(block);
        }
        // Do not retain the compressor's worst-case allocation for a small result.
        Ok(Bytes::from(compressed.into_boxed_slice()))
    }

    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes> {
        ensure!(
            (RECORD_HEADER_BYTES..=MAX_MESSAGE_BYTES).contains(&uncompress_length),
            "invalid uncompressed block length"
        );
        if block.len() == uncompress_length {
            return NoOpCodec.decode(block, uncompress_length);
        }
        ensure!(
            !block.is_empty() && block.len() < uncompress_length,
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

/// Zstd frames use the default compression level (3), with the same raw-block fallback as LZ4.
pub struct ZstdCodec;

impl Codec for ZstdCodec {
    fn encode(&self, block: Bytes) -> Result<Bytes> {
        let compressed = zstd::bulk::compress(&block, 0).context("Zstd compression failed")?;
        if compressed.len() >= block.len() {
            return NoOpCodec.encode(block);
        }
        Ok(Bytes::from(compressed.into_boxed_slice()))
    }

    fn decode(&self, block: Bytes, uncompress_length: usize) -> Result<Bytes> {
        ensure!(
            (RECORD_HEADER_BYTES..=MAX_MESSAGE_BYTES).contains(&uncompress_length),
            "invalid uncompressed block length"
        );
        if block.len() == uncompress_length {
            return NoOpCodec.decode(block, uncompress_length);
        }
        ensure!(
            !block.is_empty() && block.len() < uncompress_length,
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
