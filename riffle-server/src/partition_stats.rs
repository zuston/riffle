use crate::app_manager::request_context::{GetShuffleResultContext, ReportShuffleResultContext};
use crate::ddashmap::DDashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

/// The PartitionStatsManager will be initialized by the every app, there is no need to include app_id here.
pub struct PartitionStatsManager {
    // (shuffle_id, partition_id) -> task_attempt_id -> partition_stats
    stats: DDashMap<(i32, i32), Arc<DDashMap<i64, TaskToRecordStatRef>>>,
}

impl PartitionStatsManager {
    pub fn new() -> Self {
        Self {
            stats: Default::default(),
        }
    }

    pub fn add(&self, ctx: &ReportShuffleResultContext) -> anyhow::Result<()> {
        let shuffle_id = ctx.shuffle_id;
        let task_attempt_id = ctx.task_attempt_id;
        let records = &ctx.record_numbers;

        for record in records {
            let pid = record.0;
            let task_id_to_stats = self
                .stats
                .compute_if_absent((shuffle_id, *pid), || Default::default());
            task_id_to_stats.insert(
                task_attempt_id,
                Arc::new(TaskToRecordStat {
                    task_attempt_id,
                    record_number: *record.1,
                }),
            );
        }
        Ok(())
    }

    pub fn get(&self, ctx: &GetShuffleResultContext) -> anyhow::Result<Vec<PartitionStats>> {
        let shuffle_id = ctx.shuffle_id;
        let partition_ids = &ctx.partition_ids;

        let mut all_partition_stats = Vec::with_capacity(partition_ids.len());
        for pid in partition_ids {
            let entry = self
                .stats
                .compute_if_absent((shuffle_id, *pid), || Default::default());
            if !entry.is_empty() {
                all_partition_stats.push(PartitionStats {
                    partition_id: *pid,
                    records: entry.iter().map(|x| x.clone()).collect(),
                });
            }
        }
        Ok(all_partition_stats)
    }

    pub fn purge(&self, shuffle_id: i32) {
        let view = self.stats.clone().into_read_only();
        let mut deletion_keys = vec![];
        for (v_shuffle_id, v_partition_id) in view.keys() {
            if *v_shuffle_id == shuffle_id {
                deletion_keys.push((shuffle_id, *v_partition_id));
            }
        }
        drop(view);
        for deletion_key in deletion_keys {
            self.stats.remove(&deletion_key);
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TaskToRecordStat {
    task_attempt_id: i64,
    record_number: i64,
}

pub type TaskToRecordStatRef = Arc<TaskToRecordStat>;

impl Into<crate::grpc::protobuf::uniffle::TaskAttemptIdToRecords> for &TaskToRecordStatRef {
    fn into(self) -> crate::grpc::protobuf::uniffle::TaskAttemptIdToRecords {
        crate::grpc::protobuf::uniffle::TaskAttemptIdToRecords {
            task_attempt_id: self.task_attempt_id,
            record_number: self.record_number,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PartitionStats {
    partition_id: i32,
    records: Vec<TaskToRecordStatRef>,
}

impl Into<crate::grpc::protobuf::uniffle::PartitionStats> for PartitionStats {
    fn into(self) -> crate::grpc::protobuf::uniffle::PartitionStats {
        crate::grpc::protobuf::uniffle::PartitionStats {
            partition_id: self.partition_id,
            task_attempt_id_to_records: self.records.iter().map(|a| a.into()).collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app_manager::request_context::{
        GetShuffleResultContext, ReportShuffleResultContext,
    };
    use crate::id_layout::to_layout;
    use crate::partition_stats::PartitionStatsManager;
    use std::collections::HashMap;

    #[test]
    fn test_duplicate_task_ids() -> anyhow::Result<()> {
        let manager = PartitionStatsManager::new();
        let shuffle_id = 1999;
        let partitions = 1000;
        let records_number_per_partition = 10;

        let mut records = HashMap::new();
        for partition in 0..partitions {
            records.insert(partition, records_number_per_partition);
        }

        // add 1
        let task_attempt_id = 10;
        let report_ctx = ReportShuffleResultContext::new(
            shuffle_id,
            task_attempt_id,
            Default::default(),
            records.clone(),
        );
        manager.add(&report_ctx);

        // add 2 again
        manager.add(&report_ctx);

        // will only get 1 task_attempt_id's records
        let ctx = GetShuffleResultContext {
            shuffle_id,
            partition_ids: vec![1],
            layout: to_layout(None),
        };
        let result = manager.get(&ctx)?;
        assert_eq!(1, result.len());
        assert_eq!(1, result[0].partition_id);
        assert_eq!(1, result[0].records.len());
        assert_eq!(task_attempt_id, result[0].records[0].task_attempt_id);

        Ok(())
    }

    #[test]
    fn test_multi_tasks_report() -> anyhow::Result<()> {
        let manager = PartitionStatsManager::new();
        let shuffle_id = 1999;
        let partitions = 1000;
        let records_number_per_partition = 10;

        let mut records = HashMap::new();
        for partition in 0..partitions {
            records.insert(partition, records_number_per_partition);
        }

        // report 1 from task 10
        let task_attempt_id = 10;
        let report_ctx = ReportShuffleResultContext::new(
            shuffle_id,
            task_attempt_id,
            Default::default(),
            records.clone(),
        );
        manager.add(&report_ctx)?;

        // report 2 from task 20
        let task_attempt_id = 20;
        let report_ctx = ReportShuffleResultContext::new(
            shuffle_id,
            task_attempt_id,
            Default::default(),
            records,
        );
        manager.add(&report_ctx)?;

        // get the partition=1 with task 10/20
        let ctx = GetShuffleResultContext {
            shuffle_id,
            partition_ids: vec![1],
            layout: to_layout(None),
        };
        let result = manager.get(&ctx)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].records.len(), 2);

        let mut sorted = result[0]
            .records
            .iter()
            .map(|x| x.task_attempt_id)
            .collect::<Vec<_>>();
        assert_eq!(vec!(10, 20), sorted);

        Ok(())
    }

    #[test]
    fn test_full() -> anyhow::Result<()> {
        let manager = PartitionStatsManager::new();
        let shuffle_id = 1999;
        let partitions = 1000;
        let task_attempt_id = 50;
        let record_per_partition = 400;

        // insert
        let mut records = HashMap::new();
        for partition in 0..partitions {
            records.insert(partition, record_per_partition);
        }
        let report_ctx = ReportShuffleResultContext::new(
            shuffle_id,
            task_attempt_id,
            Default::default(),
            records,
        );
        manager.add(&report_ctx)?;

        // get all
        let ctx = GetShuffleResultContext {
            shuffle_id,
            partition_ids: (0..partitions).collect(),
            layout: to_layout(None),
        };
        let result = manager.get(&ctx)?;
        assert_eq!(result.len(), partitions as usize);
        for stat in result {
            for record in stat.records {
                assert_eq!(task_attempt_id, record.task_attempt_id);
                assert_eq!(record_per_partition, record.record_number);
            }
        }

        // get one
        let ctx = GetShuffleResultContext {
            shuffle_id,
            partition_ids: vec![0],
            layout: to_layout(None),
        };
        let result = manager.get(&ctx)?;
        assert_eq!(result.len(), 1);

        // purge
        manager.purge(shuffle_id);
        let ctx = GetShuffleResultContext {
            shuffle_id,
            partition_ids: vec![0],
            layout: to_layout(None),
        };
        let result = manager.get(&ctx)?;
        assert_eq!(result.len(), 0);

        Ok(())
    }
}
