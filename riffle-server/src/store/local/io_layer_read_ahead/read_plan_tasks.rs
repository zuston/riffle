use crate::metric::{
    READ_AHEAD_HITS_OF_READ_PLAN, READ_AHEAD_MISSES_OF_READ_PLAN,
    READ_AHEAD_OPERATION_DURATION_OF_READ_PLAN,
};
use crate::store::local::io_layer_read_ahead::do_read_ahead;
use crate::store::local::io_layer_read_ahead::processor::ReadPlanReadAheadTaskProcessor;
use crate::urpc::command::ReadSegment;
use log::{debug, info};
use parking_lot::Mutex;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct ReadPlanReadAheadTask {
    pub uid: u64,
    file: Arc<Mutex<File>>,
    sender: Arc<async_channel::Sender<ReadSegment>>,
    pub recv: Arc<async_channel::Receiver<ReadSegment>>,
    path: Arc<String>,

    processor: ReadPlanReadAheadTaskProcessor,

    // the current read offset
    read_offset: Arc<AtomicU64>,
    // the planned read offset
    plan_offset: Arc<AtomicU64>,
    // the loaded read offset
    loaded_offset: Arc<AtomicU64>,
    // missed loaded segment
    missed_counter: Arc<AtomicU64>,
    hit_counter: Arc<AtomicU64>,
}

impl ReadPlanReadAheadTask {
    pub fn new(
        abs_path: &str,
        uid: u64,
        processor: &ReadPlanReadAheadTaskProcessor,
    ) -> anyhow::Result<Self> {
        let (send, recv) = async_channel::unbounded();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(abs_path)?;
        let task = Self {
            uid,
            file: Arc::new(Mutex::new(file)),
            read_offset: Arc::new(Default::default()),
            plan_offset: Arc::new(Default::default()),
            loaded_offset: Arc::new(Default::default()),
            sender: Arc::new(send),
            recv: Arc::new(recv),
            path: Arc::new(abs_path.to_string()),
            processor: processor.clone(),
            missed_counter: Arc::new(Default::default()),
            hit_counter: Arc::new(Default::default()),
        };
        processor.add_task(&task);
        Ok(task)
    }

    pub async fn do_load(&self, segment: ReadSegment) -> anyhow::Result<()> {
        let off = segment.offset;
        let len = segment.length;

        if off < self.read_offset.load(Ordering::Relaxed) as i64 {
            self.missed_counter.fetch_add(1, Ordering::Relaxed);
            READ_AHEAD_MISSES_OF_READ_PLAN.inc();
            return Ok(());
        }
        let _timer = READ_AHEAD_OPERATION_DURATION_OF_READ_PLAN.start_timer();
        let file = self.file.lock();
        do_read_ahead(&file, self.path.as_str(), off as u64, len as u64);
        self.loaded_offset.store(off as u64, Ordering::Relaxed);
        self.hit_counter.fetch_add(1, Ordering::Relaxed);
        READ_AHEAD_HITS_OF_READ_PLAN.inc();
        Ok(())
    }

    pub async fn load(
        &self,
        next_segments: &Vec<ReadSegment>,
        now_read_off: u64,
    ) -> anyhow::Result<()> {
        debug!(
            "Offset analysis for path: {}, uid: {}. Read/Loaded/Missed: {}/{}/{}",
            &self.path,
            self.uid,
            now_read_off,
            &self.loaded_offset.load(Ordering::Relaxed),
            &self.missed_counter.load(Ordering::Relaxed)
        );

        self.read_offset.store(now_read_off, Ordering::SeqCst);

        // for plan offset
        let now_plan_offset = self.plan_offset.load(Ordering::SeqCst);
        let mut max = now_plan_offset;
        for next_segment in next_segments {
            let off = next_segment.offset as u64;
            if off > now_plan_offset {
                self.sender.send(next_segment.clone()).await?;
                max = off;
            }
        }
        self.plan_offset.store(max, Ordering::SeqCst);

        Ok(())
    }

    pub fn get_missed_count(&self) -> u64 {
        self.missed_counter.load(Ordering::Relaxed)
    }

    pub fn get_hit_count(&self) -> u64 {
        self.hit_counter.load(Ordering::Relaxed)
    }
}
