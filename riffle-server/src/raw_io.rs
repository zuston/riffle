use std::fs::File;

#[derive(Debug)]
pub struct RawIO {
    pub file: File,
    pub fd: i32,
    pub offset: u64,
    pub length: u64,
}

impl RawIO {
    pub fn new(f: File, fd: i32, offset: u64, length: u64) -> RawIO {
        Self {
            file: f,
            fd,
            offset,
            length,
        }
    }
}
