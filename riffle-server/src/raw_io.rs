#[derive(Debug)]
pub struct RawIO {
    pub fd: i32,
    pub offset: u64,
    pub length: Option<u64>,
}

impl RawIO {
    pub fn new(fd: i32, offset: u64, length: Option<u64>) -> RawIO {
        Self { fd, offset, length }
    }
}
