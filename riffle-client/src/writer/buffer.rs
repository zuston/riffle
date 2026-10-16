use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use super::memory_tracker::MemoryReservation;

use crate::RECORD_HEADER_BYTES;

pub(super) const INITIAL_CAPACITY_BYTES: usize = 1024 * 1024;
const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 14;

pub struct Buffer {
    data: BytesMut,
    used: usize,
    memory: MemoryReservation,
}

impl Buffer {
    pub fn new(memory: MemoryReservation) -> Self {
        Self {
            data: BytesMut::with_capacity(memory.size()),
            used: 0,
            memory,
        }
    }

    pub fn append(&mut self, record: &Bytes) -> Result<bool> {
        let required = self.data.len() + RECORD_HEADER_BYTES + record.len();
        if required > self.memory.size() {
            let capacity = (required.div_ceil(INITIAL_CAPACITY_BYTES) * INITIAL_CAPACITY_BYTES)
                .max(self.data.capacity() * 2);
            if !self.memory.try_grow(capacity)? {
                return Ok(false);
            }
            // Reserve once so the header and payload cannot trigger separate growth.
            self.data.reserve(capacity - self.data.len());
        }
        self.data.put_u32(record.len() as u32);
        self.data.extend_from_slice(record);
        self.used += record.len() + RECORD_HEADER_BYTES;
        debug_assert!(self.data.capacity() <= self.memory.size());
        Ok(true)
    }

    pub fn is_full(&self) -> bool {
        self.used >= MAX_BUFFER_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn clear(self) -> Option<(Bytes, MemoryReservation)> {
        if self.is_empty() {
            return None;
        }
        Some((self.data.freeze(), self.memory))
    }
}
