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

pub struct ReadOptions {
    pub io_mode: IoMode,
    pub read_range: ReadRange,
    pub sequential: bool,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum ReadRange {
    ALL,
    // offset, length
    RANGE(u64, u64),
}

#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum IoMode {
    SENDFILE,
    DIRECT_IO,
    BUFFER_IO,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: ReadRange::ALL,
            sequential: false,
        }
    }
}

impl ReadOptions {
    pub fn new(io_mode: IoMode, range: ReadRange, is_sequential: bool) -> Self {
        Self {
            io_mode,
            read_range: range,
            sequential: is_sequential,
        }
    }

    pub fn with_read_all(self) -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: ReadRange::ALL,
            sequential: false,
        }
    }

    pub fn with_read_range(self, range: ReadRange) -> Self {
        Self {
            io_mode: self.io_mode,
            read_range: range,
            sequential: self.sequential,
        }
    }

    pub fn with_buffer_io(self) -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }

    pub fn with_direct_io(self) -> Self {
        Self {
            io_mode: IoMode::DIRECT_IO,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }

    pub fn with_sendfile(self) -> Self {
        Self {
            io_mode: IoMode::SENDFILE,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }
}

pub enum CreateOptions {
    FILE,
    DIR,
}
