use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

pub struct LazyInit<T> {
    initializer: Mutex<Option<Box<dyn FnOnce() -> Option<T> + Send>>>,
    value: OnceCell<Option<T>>,
    initialized: AtomicBool,
}

impl<T> LazyInit<T> {
    pub fn new<F>(initializer: F) -> Self
    where
        F: FnOnce() -> Option<T> + Send + 'static,
    {
        LazyInit {
            initializer: Mutex::new(Some(Box::new(initializer))),
            value: OnceCell::new(),
            initialized: AtomicBool::new(false),
        }
    }

    pub fn get_or_init(&self) -> Option<&T> {
        let v = self.value.get_or_init(|| {
            let initializer = self.initializer.lock().take().unwrap();
            let v = initializer();
            self.initialized.store(true, SeqCst);
            v
        });
        v.as_ref()
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use crate::lazy_initializer::LazyInit;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;

    #[test]
    fn test() {
        let tag = Arc::new(AtomicU64::new(0));
        let tag_fork = tag.clone();
        let lazy_value = LazyInit::new(move || {
            println!("Initializing...");
            tag_fork.fetch_add(1, SeqCst);
            Some(())
        });

        let value = lazy_value.get_or_init();
        let value = lazy_value.get_or_init();

        assert_eq!(tag.load(SeqCst), 1);
    }
}
