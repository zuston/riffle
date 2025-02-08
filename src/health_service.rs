use crate::app::AppManagerRef;
use crate::config::HealthServiceConfig;
use crate::mem_allocator::ALLOCATOR;
use crate::storage::HybridStorage;
use anyhow::Result;
use bytesize::ByteSize;
use dashmap::DashMap;
use libc::passwd;
use log::{info, warn};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealthService {
    app_manager_ref: AppManagerRef,
    hybrid_storage: HybridStorage,

    alive_app_number_limit: Option<usize>,
    disk_used_ratio_health_threshold: Option<f64>,
    memory_allocated_threshold: Option<u64>,

    health_stat: Arc<HealthStat>,
}

struct HealthStat {
    s_1: AtomicBool,
    s_2: AtomicBool,
    s_3: AtomicBool,
    s_4: AtomicBool,
}

impl Default for HealthStat {
    fn default() -> Self {
        Self {
            s_1: AtomicBool::new(true),
            s_2: AtomicBool::new(true),
            s_3: AtomicBool::new(true),
            s_4: AtomicBool::new(true),
        }
    }
}

fn parse(s: &str) -> u64 {
    s.parse::<ByteSize>().unwrap().0
}

impl HealthService {
    pub fn new(
        app_manager: &AppManagerRef,
        storage: &HybridStorage,
        conf: &HealthServiceConfig,
    ) -> Self {
        let memory_allocated_threshold = match &conf.memory_allocated_threshold {
            Some(threshold) => Some(parse(threshold)),
            _ => None,
        };
        if let Some(val) = &memory_allocated_threshold {
            info!(
                "The memory allocated threshold has been activated. threshold: {}",
                val
            );
        }

        Self {
            app_manager_ref: app_manager.clone(),
            hybrid_storage: storage.clone(),
            alive_app_number_limit: conf.alive_app_number_max_limit,
            disk_used_ratio_health_threshold: conf.disk_used_ratio_health_threshold,
            memory_allocated_threshold,
            health_stat: Arc::new(Default::default()),
        }
    }

    pub async fn is_healthy(&self) -> Result<bool> {
        if let Some(disk_used_ratio_health_threshold) = self.disk_used_ratio_health_threshold {
            let localfile_stat = self
                .app_manager_ref
                .store_localfile_stat()?
                .is_healthy(disk_used_ratio_health_threshold);
            let prev_stat = self.health_stat.s_1.load(SeqCst);
            if prev_stat != localfile_stat {
                warn!(
                    "The health state from checker of [disk used ratio] changes from [{}] to [{}]",
                    prev_stat, localfile_stat
                );
                self.health_stat.s_1.store(!prev_stat, SeqCst);
            }
            if !localfile_stat {
                return Ok(false);
            }
        }

        let stat = self.app_manager_ref.store_is_healthy().await?;
        let prev_stat = self.health_stat.s_2.load(SeqCst);
        if prev_stat != stat {
            warn!(
                "The health state from checker of [hybrid store health] changes from [{}] to [{}]",
                prev_stat, stat
            );
            self.health_stat.s_2.store(!prev_stat, SeqCst);
        }
        if !stat {
            return Ok(false);
        }

        // for the initial deploy, to ensure the service stable.
        // this could be removed in the future.
        // case1: app number limit
        // case2: once disk unhealthy, mark the service unhealthy

        if let Some(limit) = self.alive_app_number_limit {
            let alive_app_number = self.app_manager_ref.get_alive_app_number();
            if alive_app_number > limit {
                return Ok(false);
            }
        }

        #[cfg(all(unix, feature = "allocator-analysis"))]
        {
            if let Some(threshold) = self.memory_allocated_threshold {
                if !self.health_stat.s_4.load(SeqCst) {
                    return Ok(false);
                }

                let allocated = ALLOCATOR.allocated();
                if (allocated > threshold as usize) {
                    self.health_stat.s_4.store(false, SeqCst);
                    warn!("Mark the service unhealthy due to exceeding the memory allocated threshold");
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
