use crate::await_tree::AwaitTreeDelegator;
use bumpalo::Bump;
use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::Arc;

pub static ARENA_ALLOCATOR: Lazy<ArenaAllocator> = Lazy::new(|| ArenaAllocator::new());

pub struct ArenaAllocator {
    allocator: Arc<Mutex<Bump>>,
}

impl ArenaAllocator {
    pub fn new() -> Self {
        Self {
            allocator: Arc::new(Mutex::new(Bump::with_capacity(1024 * 1024 * 1024 * 10))),
        }
    }

    pub fn slice_copy(&self, slice: &[u8]) -> Bytes {
        let binding = self.allocator.lock();
        let data = binding.alloc_slice_copy(slice);
        let static_ref: &'static [u8] = unsafe { std::mem::transmute(data) };
        Bytes::from_static(static_ref)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use bumpalo::Bump;
    use bytes::Bytes;

    #[test]
    fn test_allocation() {
        let bump = Bump::with_capacity(1024);
        let bytes_1 = bump.alloc(Bytes::copy_from_slice(&[1u8; 1024]));
        let bytes_2 = bump.alloc(Bytes::copy_from_slice(&[1u8; 1024]));
    }
}
