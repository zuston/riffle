use crate::composed_bytes::recycle_blocks::{Block, BlockManager};
use crate::store::alignment::io_buffer_pool::IoBufferPool;
use crate::store::alignment::ALIGN;
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use std::ops::{Deref, DerefMut};

const BLOCK_SIZE: usize = 16 * 1024 * 1024;
const POOL_CAPACITY: usize = 4 * 1024 * 1024 * 1024 / BLOCK_SIZE;
static BLOCK_MANAGER: Lazy<BlockManager> =
    Lazy::new(|| BlockManager::new(BLOCK_SIZE, POOL_CAPACITY));

/// To compose multi Bytes into one for zero copy.
#[derive(Clone, Debug)]
pub struct ComposedBytes {
    composed: Vec<Bytes>,
    total_len: usize,
}

impl ComposedBytes {
    pub fn new() -> ComposedBytes {
        Self {
            composed: vec![],
            total_len: 0,
        }
    }

    pub fn from(all: Vec<Bytes>, total_size: usize) -> ComposedBytes {
        Self {
            composed: all,
            total_len: total_size,
        }
    }

    pub fn put(&mut self, bytes: Bytes) {
        self.total_len += bytes.len();
        self.composed.push(bytes);
    }

    /// Freeze the composed bytes into a single immutable Bytes object.
    pub fn freeze(&self) -> Bytes {
        let mut block = BLOCK_MANAGER.acquire(self.total_len);
        for x in self.composed.iter() {
            block.extend_from_slice(x);
        }
        let data = block.clone().freeze();
        data
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> + '_ {
        self.composed.iter()
    }

    pub fn to_vec(self) -> Vec<Bytes> {
        self.composed
    }

    pub fn len(&self) -> usize {
        self.total_len
    }
}

pub mod recycle_blocks {
    use crate::store::alignment::io_buffer_pool::RecycledIoBuffer;
    use crate::store::alignment::io_bytes::IoBuffer;
    use bytes::BytesMut;
    use crossbeam::queue::ArrayQueue;
    use std::ops::{Deref, DerefMut};

    #[derive(Debug)]
    pub struct BlockManager {
        capacity: usize,
        block_size: usize,
        queue: ArrayQueue<Block>,
    }

    impl BlockManager {
        pub fn new(block_size: usize, capacity: usize) -> Self {
            Self {
                capacity,
                block_size,
                queue: ArrayQueue::new(capacity),
            }
        }

        pub fn acquire(&self, size: usize) -> RecycledBlock {
            if size > self.block_size {
                return RecycledBlock {
                    internal: Block {
                        bytes_mut: BytesMut::with_capacity(size),
                    },
                    pool: None,
                };
            }

            let create = || Block::new(self.block_size);
            let block = match self.queue.pop() {
                Some(block) => block,
                None => create(),
            };
            RecycledBlock {
                internal: block,
                pool: Some(self),
            }
        }

        pub fn release(&self, block: Block) {
            if self.queue.len() < self.capacity {
                let _ = self.queue.push(block);
            }
        }

        pub fn block_size(&self) -> usize {
            self.block_size
        }
    }

    #[derive(Default, Debug)]
    pub struct Block {
        bytes_mut: BytesMut,
    }

    impl From<BytesMut> for Block {
        fn from(value: BytesMut) -> Self {
            Self { bytes_mut: value }
        }
    }
    impl Block {
        fn new(size: usize) -> Self {
            Self {
                bytes_mut: BytesMut::with_capacity(size),
            }
        }
    }

    impl DerefMut for Block {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.bytes_mut
        }
    }

    impl Deref for Block {
        type Target = BytesMut;

        fn deref(&self) -> &Self::Target {
            &self.bytes_mut
        }
    }

    pub struct RecycledBlock<'a> {
        internal: Block,
        pool: Option<&'a BlockManager>,
    }

    impl Drop for RecycledBlock<'_> {
        fn drop(&mut self) {
            if let Some(pool) = self.pool {
                let mut taked = std::mem::take(&mut self.internal);
                taked.bytes_mut.clear();
                pool.release(taked);
            }
        }
    }

    impl Deref for RecycledBlock<'_> {
        type Target = Block;

        fn deref(&self) -> &Self::Target {
            &self.internal
        }
    }

    impl DerefMut for RecycledBlock<'_> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.internal
        }
    }
}

#[cfg(test)]
mod test {
    use crate::composed_bytes::ComposedBytes;
    use bytes::Bytes;

    #[test]
    fn test_bytes() {
        let mut composed = ComposedBytes::new();
        composed.put(Bytes::copy_from_slice(b"hello"));
        composed.put(Bytes::copy_from_slice(b"world"));
        assert_eq!(10, composed.len());

        let mut iter = composed.iter();
        assert_eq!(b"hello", iter.next().unwrap().as_ref());
        assert_eq!(b"world", iter.next().unwrap().as_ref());

        let data = composed.freeze();
        assert_eq!(b"helloworld", data.as_ref());
    }
}
