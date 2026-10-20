use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tokio::task::JoinHandle;

pub struct ScheduledTask {
    task: Mutex<Option<JoinHandle<()>>>,
}

impl ScheduledTask {
    pub fn new<F, Fut>(interval: Duration, mut function: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                function().await;
            }
        });
        Self {
            task: Mutex::new(Some(task)),
        }
    }

    pub async fn close(&self) {
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ScheduledTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}
