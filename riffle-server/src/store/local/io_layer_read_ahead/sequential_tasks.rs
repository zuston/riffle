use crate::metric::READ_AHEAD_OPERATION_DURATION_OF_SEQUENTIAL;
use crate::store::local::io_layer_read_ahead::do_read_ahead;
use std::fs::File;
use std::sync::Arc;

#[derive(Clone)]
pub struct SequentialReadAheadTask {
    inner: Arc<tokio::sync::Mutex<Inner>>,
}

struct Inner {
    absolute_path: String,
    file: File,

    is_initialized: bool,
    load_start_offset: u64,
    load_length: u64,

    batch_size: usize,
    batch_number: usize,
}

impl SequentialReadAheadTask {
    pub fn new(abs_path: &str, batch_size: usize, batch_number: usize) -> anyhow::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(abs_path)?;
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(Inner {
                absolute_path: abs_path.to_string(),
                file,
                is_initialized: false,
                load_start_offset: 0,
                load_length: 0,
                batch_size,
                batch_number,
            })),
        })
    }

    // the return value shows whether the load operation happens
    pub async fn load(&self, off: u64, len: u64) -> anyhow::Result<bool> {
        let mut inner = self.inner.lock().await;
        if !inner.is_initialized && off == 0 {
            let load_len = (inner.batch_number * inner.batch_size) as u64;
            do_read_ahead(&inner.file, inner.absolute_path.as_str(), 0, load_len);
            inner.is_initialized = true;
            inner.load_length = load_len;
            return Ok(true);
        }

        let diff = inner.load_length - off;
        let next_load_bytes = 2 * inner.batch_size as u64;
        if diff > 0 && diff < next_load_bytes {
            let _timer = READ_AHEAD_OPERATION_DURATION_OF_SEQUENTIAL.start_timer();
            let load_len = next_load_bytes;
            do_read_ahead(
                &inner.file,
                inner.absolute_path.as_str(),
                inner.load_start_offset + inner.load_length,
                load_len,
            );
            inner.load_length += load_len;
            return Ok(true);
        }

        Ok(false)
    }
}
