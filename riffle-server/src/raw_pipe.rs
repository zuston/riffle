use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};

#[derive(Debug)]
pub struct RawPipe {
    pub pipe_in_fd: File,
    pub pipe_out_fd: File,
    pub length: usize,
}

impl RawPipe {
    pub fn from(pipe_in_fd: File, pipe_out_fd: File, length: usize) -> Self {
        Self {
            pipe_in_fd,
            pipe_out_fd,
            length,
        }
    }
}
