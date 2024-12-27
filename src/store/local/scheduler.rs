use crate::config::IoSchedulerConfig;
use crate::metric::{
    IO_SCHEDULER_APPEND_PERMITS, IO_SCHEDULER_APPEND_WAIT, IO_SCHEDULER_READ_PERMITS,
    IO_SCHEDULER_READ_WAIT, IO_SCHEDULER_SHARED_PERMITS,
};
use await_tree::InstrumentAwait;
use log::info;
use prometheus::IntGaugeVec;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

pub struct IoScheduler {
    bandwidth: usize,
    read_buffer: Semaphore,
    append_buffer: Semaphore,
    shared_buffer: Semaphore,
    root: String,
}

#[derive(Debug, strum_macros::Display)]
pub enum IoType {
    READ,
    APPEND,
}

pub struct IoPermit<'a> {
    internal: SemaphorePermit<'a>,
    metric: IntGaugeVec,
    root: &'a str,
}

impl<'a> IoPermit<'a> {
    pub fn new(root: &'a str, permit: SemaphorePermit<'a>, metric: IntGaugeVec) -> Self {
        metric.with_label_values(&[root]).inc();
        Self {
            internal: permit,
            metric,
            root,
        }
    }
}

impl<'a> Drop for IoPermit<'a> {
    fn drop(&mut self) {
        self.metric.with_label_values(&[self.root]).dec();
    }
}

impl IoScheduler {
    pub fn new(root: &str, io_scheduler_config: &Option<IoSchedulerConfig>) -> IoScheduler {
        let (bandwidth, read_ratio, append_ratio, shared_ratio) = match io_scheduler_config {
            Some(io_scheduler) => (
                io_scheduler.disk_bandwidth,
                io_scheduler.read_buffer_ratio,
                io_scheduler.append_buffer_ratio,
                io_scheduler.shared_buffer_ratio,
            ),
            _ => (1024 * 1024 * 1024, 0.5, 0.5, 0.5),
        };

        info!("Initialized io scheduler with disk bandwidth {} of disk: {}. read: {}, append: {}, shared: {}", bandwidth, root, read_ratio, append_ratio, shared_ratio);
        let read_buffer = Semaphore::new((bandwidth as f64 * read_ratio) as usize);
        let append_buffer = Semaphore::new((bandwidth as f64 * append_ratio) as usize);
        let shared_buffer = Semaphore::new((bandwidth as f64 * shared_ratio) as usize);
        Self {
            bandwidth,
            read_buffer,
            append_buffer,
            shared_buffer,
            root: root.to_owned(),
        }
    }

    pub async fn acquire(
        &self,
        io_type: IoType,
        batch_bytes: usize,
    ) -> Result<IoPermit<'_>, AcquireError> {
        let (buffer, mut permit_metric, wait_metric) = match io_type {
            IoType::READ => (
                &self.read_buffer,
                IO_SCHEDULER_READ_PERMITS.clone(),
                IO_SCHEDULER_READ_WAIT.clone(),
            ),
            IoType::APPEND => (
                &self.append_buffer,
                IO_SCHEDULER_APPEND_PERMITS.clone(),
                IO_SCHEDULER_APPEND_WAIT.clone(),
            ),
        };

        wait_metric.with_label_values(&[&self.root]).inc();
        let permit = if batch_bytes > buffer.available_permits()
            && batch_bytes <= self.shared_buffer.available_permits()
        {
            permit_metric = IO_SCHEDULER_SHARED_PERMITS.clone();
            self.shared_buffer
                .acquire_many(batch_bytes as u32)
                .instrument_await(format!(
                    "Shared buffer wait in io scheduler:[{}]",
                    &self.root
                ))
                .await?
        } else {
            buffer
                .acquire_many(batch_bytes as u32)
                .instrument_await(format!(
                    "{} buffer wait in io scheduler:[{}]",
                    &io_type, &self.root
                ))
                .await?
        };
        wait_metric.with_label_values(&[&self.root]).dec();

        Ok(IoPermit::new(&self.root, permit, permit_metric))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::IoSchedulerConfig;
    use crate::store::local::scheduler::{IoScheduler, IoType};
    use anyhow::Result;

    #[tokio::test]
    async fn test_permit() -> Result<()> {
        let scheduler = IoScheduler::new(
            "/tmp",
            &Some(IoSchedulerConfig {
                disk_bandwidth: 10,
                read_buffer_ratio: 0.5,
                append_buffer_ratio: 0.5,
                shared_buffer_ratio: 0.5,
            }),
        );
        let read_permit_1 = scheduler.acquire(IoType::READ, 4).await?;
        let read_permit_2 = scheduler.acquire(IoType::READ, 5).await?;
        let append_permit_1 = scheduler.acquire(IoType::APPEND, 4).await?;

        assert_eq!(1, scheduler.read_buffer.available_permits());
        assert_eq!(1, scheduler.append_buffer.available_permits());
        assert_eq!(0, scheduler.shared_buffer.available_permits());

        drop(read_permit_2);
        assert_eq!(5, scheduler.shared_buffer.available_permits());

        let append_permit_2 = scheduler.acquire(IoType::APPEND, 4).await?;
        assert_eq!(1, scheduler.shared_buffer.available_permits());

        Ok(())
    }
}
