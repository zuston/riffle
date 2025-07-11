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
    pub sendfile: bool,
    pub direct_io: bool,
    pub offset: u64,
    pub length: Option<u64>,
}

impl ReadOptions {
    // reading all from the file.
    pub fn with_read_all() -> Self {
        Self {
            sendfile: false,
            direct_io: false,
            offset: 0,
            length: None,
        }
    }

    pub fn with_read_of_direct_io(offset: u64, length: u64) -> Self {
        Self {
            sendfile: false,
            direct_io: true,
            offset,
            length: Some(length),
        }
    }

    pub fn with_read_of_buffer_io(offset: u64, length: u64) -> Self {
        Self {
            sendfile: false,
            direct_io: false,
            offset,
            length: Some(length),
        }
    }

    pub fn with_sendfile(offset: u64, length: u64) -> Self {
        Self {
            sendfile: true,
            direct_io: false,
            offset,
            length: Some(length),
        }
    }

    pub fn is_read_all(&self) -> bool {
        self.length.is_none()
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    pub fn is_sendfile(&self) -> bool {
        self.sendfile
    }
}

pub enum CreateOptions {
    FILE,
    DIR,
}
