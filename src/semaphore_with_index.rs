use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

pub struct SemaphoreWithIndex {
    internal: Semaphore,
    permits: usize,
    index_ref: Arc<AtomicUsize>,
}

pub struct SemaphorePermitWithIndex<'a> {
    internal: SemaphorePermit<'a>,
    index: usize,
    index_ref: Arc<AtomicUsize>,
}

impl<'a> SemaphorePermitWithIndex<'a> {
    pub fn get_index(&self) -> usize {
        self.index
    }
}

impl<'a> Drop for SemaphorePermitWithIndex<'a> {
    fn drop(&mut self) {
        self.index_ref.fetch_sub(1, SeqCst);
    }
}

impl SemaphoreWithIndex {
    pub fn new(permits: usize) -> Self {
        Self {
            internal: Semaphore::new(permits),
            permits,
            index_ref: Default::default(),
        }
    }

    pub async fn acquire(&self) -> Result<SemaphorePermitWithIndex<'_>, AcquireError> {
        let permit = self.internal.acquire().await?;
        let index_ref = self.index_ref.clone();
        let index = index_ref.fetch_add(1, SeqCst);
        Ok(SemaphorePermitWithIndex {
            internal: permit,
            index,
            index_ref,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::semaphore_with_index::SemaphoreWithIndex;
    use std::sync::atomic::Ordering::SeqCst;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let semaphore = SemaphoreWithIndex::new(2);

        let permit1 = semaphore.acquire().await?;
        assert_eq!(0, permit1.index);

        let permit2 = semaphore.acquire().await?;
        assert_eq!(1, permit2.index);

        assert_eq!(2, semaphore.index_ref.load(SeqCst));

        drop(permit1);
        assert_eq!(1, semaphore.index_ref.load(SeqCst));

        drop(permit2);
        assert_eq!(0, semaphore.index_ref.load(SeqCst));

        Ok(())
    }
}
