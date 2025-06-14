use crate::app_manager::AppManagerRef;
use crate::config::Config;
use crate::grpc::protobuf::uniffle::ServerStatus;
use crate::util;
use log::{info, warn};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde::Deserialize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use strum_macros::{Display, EnumString};

const INTERVAL: u64 = 60 * 10;

pub static SERVER_STATE_MANAGER_REF: OnceCell<ServerStateManager> = OnceCell::new();

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Deserialize, EnumString, Display)]
pub enum ServerState {
    ACTIVE,
    DECOMMISSIONING,
    CANCEL_DECOMMISSION,
    DECOMMISSIONED,
    UNHEALTHY,
    HEALTHY,
}

#[derive(Clone)]
pub struct ServerStateManager {
    state: Arc<RwLock<ServerState>>,
    state_time: Arc<AtomicU64>,
    app_manager_ref: AppManagerRef,

    kill_interval: Arc<AtomicU64>,
    kill_signal_send_enable: Arc<AtomicBool>,
}

impl ServerStateManager {
    pub fn new(app_manager: &AppManagerRef, config: &Config) -> ServerStateManager {
        let initial_status = if config.initial_unhealthy_status_enable {
            info!("Making server as unhealthy status due to the implicit setting");
            ServerState::UNHEALTHY
        } else {
            ServerState::ACTIVE
        };
        ServerStateManager {
            state: Arc::new(RwLock::new(initial_status)),
            state_time: Arc::new(AtomicU64::new(util::now_timestamp_as_sec())),
            app_manager_ref: app_manager.clone(),
            kill_interval: Arc::new(AtomicU64::new(INTERVAL)),
            kill_signal_send_enable: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn shutdown(&self, force: bool) -> anyhow::Result<()> {
        if !force {
            let alive_apps = self.app_manager_ref.get_alive_app_number();
            if alive_apps > 0 {
                return anyhow::bail!("Still {} apps", alive_apps);
            }
        }
        info!("Shutting down server...");
        send_sigterm_to_self();
        Ok(())
    }

    pub fn as_state(&self, state: ServerState) {
        let mut internal_state = self.state.write();
        if *internal_state == state {
            warn!("Same with internal server status. Ignoring");
            return;
        }
        info!("Transition from {} to {}", internal_state, state);
        *internal_state = state;
        self.state_time.store(util::now_timestamp_as_sec(), SeqCst);
    }

    fn get_state(&self) -> ServerState {
        self.state.read().clone()
    }

    /// This method will be invoked periodically by heartbeat task
    pub fn get_server_status(&self) -> ServerStatus {
        let internal_state = self.get_state();

        // Due to the bug of uniffle coordinator, the decommissioning status is not valid.
        // So we have to set it as unhealthy. Tracking this problem in
        // https://github.com/apache/uniffle/issues/2443
        let server_status = match internal_state {
            ServerState::ACTIVE => ServerStatus::Active,
            ServerState::DECOMMISSIONING => ServerStatus::Unhealthy,
            ServerState::DECOMMISSIONED => ServerStatus::Unhealthy,
            ServerState::CANCEL_DECOMMISSION => ServerStatus::Active,
            ServerState::UNHEALTHY => ServerStatus::Unhealthy,
            ServerState::HEALTHY => ServerStatus::Active,
        };

        if internal_state == ServerState::DECOMMISSIONING
            && self.app_manager_ref.get_alive_app_number() <= 0
            && util::now_timestamp_as_sec() - self.state_time.load(SeqCst)
                > self.kill_interval.load(SeqCst)
        {
            self.as_state(ServerState::DECOMMISSIONED);
            info!("Decommission success and then to kill");

            if self.kill_signal_send_enable.load(SeqCst) {
                send_sigterm_to_self();
            }
        }

        server_status
    }

    fn set_kill_interval(&self, interval: u64) {
        self.kill_interval.store(interval, SeqCst);
    }

    fn disable_kill_signal(&self) {
        self.kill_signal_send_enable.store(false, SeqCst);
    }
}

fn send_sigterm_to_self() {
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
}

#[cfg(test)]
mod tests {
    use crate::app_manager::test::mock_config;
    use crate::app_manager::AppManager;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::grpc::protobuf::uniffle::ServerStatus;
    use crate::runtime::manager::RuntimeManager;
    use crate::server_state_manager::{ServerState, ServerStateManager};
    use crate::storage::StorageService;
    use anyhow::Result;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_macro() -> Result<()> {
        let status = ServerState::CANCEL_DECOMMISSION;
        assert_eq!("CANCEL_DECOMMISSION", status.to_string());
        Ok(())
    }

    #[test]
    fn test_decommission() -> Result<()> {
        let app_id = "test_decommission-----id".to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();

        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref = AppManager::get_ref(
            runtime_manager.clone(),
            config.clone(),
            &storage,
            &reconf_manager,
        )
        .clone();

        let server_state_manager = ServerStateManager::new(&app_manager_ref, &config);

        // case1
        server_state_manager.as_state(ServerState::DECOMMISSIONING);
        assert_eq!(
            ServerState::DECOMMISSIONING,
            server_state_manager.get_state()
        );
        // due to the uniffle's bug
        assert_eq!(
            ServerStatus::Unhealthy,
            server_state_manager.get_server_status()
        );

        // case2
        server_state_manager.set_kill_interval(1);
        server_state_manager.disable_kill_signal();
        awaitility::at_most(Duration::from_secs(2)).until(|| {
            assert_eq!(
                server_state_manager.get_server_status(),
                ServerStatus::Unhealthy
            );
            server_state_manager.get_state() == ServerState::DECOMMISSIONED
        });

        Ok(())
    }
}
