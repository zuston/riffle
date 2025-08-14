use crate::app_manager::request_context::PurgeDataContext;
use crate::store::DataBytes;

/// If the append is false, the offset should be None.
/// Otherwise, if the offset is None, it indicates using buffer io.
/// Otherwise, it uses the direct IO
pub struct WriteOptions {
    pub append: bool,
    pub data: DataBytes,
    // if the offset is empty, using the buffer io otherwise direct io.
    pub offset: Option<u64>,
}

impl WriteOptions {
    pub fn with_write_all(data: DataBytes) -> Self {
        Self {
            append: false,
            data,
            offset: None,
        }
    }

    pub fn with_append_of_direct_io(data: DataBytes, offset: u64) -> Self {
        Self {
            append: true,
            data,
            offset: Some(offset),
        }
    }

    pub fn with_append_of_buffer_io(data: DataBytes) -> Self {
        Self {
            append: true,
            data,
            offset: None,
        }
    }

    pub fn is_append(&self) -> bool {
        self.append
    }

    pub fn is_direct_io(&self) -> bool {
        self.offset.is_some()
    }
}

pub enum CreateOptions {
    FILE,
    DIR,
}
