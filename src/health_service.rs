use crate::app::AppManagerRef;
use crate::config::HealthServiceConfig;
use crate::storage::HybridStorage;
use anyhow::Result;

#[derive(Clone)]
pub struct HealthService {
    app_manager_ref: AppManagerRef,
    hybrid_storage: HybridStorage,

    alive_app_number_limit: Option<usize>,
    disk_used_ratio_health_threshold: Option<f64>,
}

impl HealthService {
    pub fn new(
        app_manager: &AppManagerRef,
        storage: &HybridStorage,
        conf: &HealthServiceConfig,
    ) -> Self {
        Self {
            app_manager_ref: app_manager.clone(),
            hybrid_storage: storage.clone(),
            alive_app_number_limit: conf.alive_app_number_max_limit,
            disk_used_ratio_health_threshold: conf.disk_used_ratio_health_threshold,
        }
    }

    pub async fn is_healthy(&self) -> Result<bool> {
        if let Some(disk_used_ratio_health_threshold) = self.disk_used_ratio_health_threshold {
            let localfile_stat = self.app_manager_ref.store_localfile_stat()?;
            if !localfile_stat.is_healthy(disk_used_ratio_health_threshold) {
                return Ok(false);
            }
        }

        if !self.app_manager_ref.store_is_healthy().await? {
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

        Ok(true)
    }
}
