use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};

#[derive(Debug)]
pub struct RawIO {
    pub file: File,
    pub offset: u64,
    pub length: u64,
    pub raw_fd: RawFd,
}

impl RawIO {
    pub fn new(f: File, offset: u64, length: u64) -> RawIO {
        let raw_fd = f.as_raw_fd();
        Self {
            file: f,
            offset,
            length,
            raw_fd,
        }
    }
}
