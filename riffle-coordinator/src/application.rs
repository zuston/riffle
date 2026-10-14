// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;
use riffle_server::app_manager::app_stats::{AppStats, AppStatsSnapshot};
use riffle_server::grpc::protobuf::uniffle;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_APP_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DEFAULT_APP_EXPIRATION_CHECK_INTERVAL: Duration = Duration::from_secs(60);

pub type ServerId = String;

#[derive(Clone, Debug)]
pub struct ApplicationInfo {
    pub app_id: String,
    pub user: String,
    // todo: add client version + git commit id
    pub last_heartbeat_time: DateTime<Utc>,
    pub start_time: DateTime<Utc>,

    // app stats with shuffle server_id
    pub stats: DashMap<ServerId, AppStats>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApplicationSnapshot {
    pub app_id: String,
    pub user: String,
    pub last_heartbeat_time: DateTime<Utc>,
    pub start_time: DateTime<Utc>,
    pub stats: AppStatsSnapshot,
}

impl ApplicationInfo {
    pub fn new(app_id: String, user: String) -> Self {
        let now = Utc::now();
        Self {
            app_id,
            user,
            last_heartbeat_time: now,
            start_time: now,
            stats: Default::default(),
        }
    }

    pub fn snapshot(&self) -> ApplicationSnapshot {
        let mut stats = AppStatsSnapshot::default();
        for entry in self.stats.iter() {
            let snapshot = entry.value().snapshot();
            stats.received_bytes = stats.received_bytes.saturating_add(snapshot.received_bytes);
            stats.resident_bytes = stats.resident_bytes.saturating_add(snapshot.resident_bytes);
            stats.memory_bytes = stats.memory_bytes.saturating_add(snapshot.memory_bytes);
            stats.localfile_bytes = stats
                .localfile_bytes
                .saturating_add(snapshot.localfile_bytes);
            stats.hdfs_bytes = stats.hdfs_bytes.saturating_add(snapshot.hdfs_bytes);
        }

        ApplicationSnapshot {
            app_id: self.app_id.clone(),
            user: self.user.clone(),
            last_heartbeat_time: self.last_heartbeat_time,
            start_time: self.start_time,
            stats,
        }
    }
}

#[derive(Clone)]
pub struct ApplicationManager {
    apps: Arc<DashMap<String, ApplicationInfo>>,
    heartbeat_timeout: Duration,
    expiration_check_interval: Duration,
    expiration_check_lock: Arc<parking_lot::Mutex<Instant>>,
}

impl Default for ApplicationManager {
    fn default() -> Self {
        Self::new(
            DEFAULT_APP_HEARTBEAT_TIMEOUT,
            DEFAULT_APP_EXPIRATION_CHECK_INTERVAL,
        )
    }
}

impl ApplicationManager {
    pub fn new(heartbeat_timeout: Duration, expiration_check_interval: Duration) -> Self {
        Self {
            apps: Arc::new(DashMap::new()),
            heartbeat_timeout,
            expiration_check_interval,
            expiration_check_lock: Arc::new(parking_lot::Mutex::new(Instant::now())),
        }
    }

    pub fn register(&self, app_id: String, user: String) {
        self.apps.entry(app_id.to_string()).or_insert_with(|| {
            info!("Application registered: {app_id} (user: {user})");
            ApplicationInfo::new(app_id.to_string(), user)
        });
    }

    pub fn heartbeat(&self, app_id: &str) {
        // update the application's last heartbeat time if it exists
        if let Some(mut application) = self.apps.get_mut(app_id) {
            application.last_heartbeat_time = Utc::now();
        }

        // trigger the removal of expired applications
        self.remove_expired();
    }

    fn remove_expired(&self) {
        let Some(mut last_check) = self.expiration_check_lock.try_lock() else {
            return;
        };
        if last_check.elapsed() < self.expiration_check_interval {
            return;
        }

        let now = Utc::now();
        self.apps.retain(|_, application| {
            now.signed_duration_since(application.last_heartbeat_time)
                .to_std()
                .map_or(true, |age| age < self.heartbeat_timeout)
        });
        *last_check = Instant::now();
    }

    pub fn update_app_stats(&self, app_infos: Vec<uniffle::ApplicationInfo>, server_id: &ServerId) {
        for app_info in app_infos {
            if let Some(app) = self.apps.get(&app_info.app_id) {
                app.stats
                    .insert(server_id.clone(), AppStats::from(app_info));
            }
        }
    }

    pub fn snapshot(&self, app_id: &str) -> Option<ApplicationSnapshot> {
        self.remove_expired();
        self.apps.get(app_id).map(|app| app.snapshot())
    }

    pub fn snapshots(&self) -> Vec<ApplicationSnapshot> {
        self.remove_expired();
        let mut snapshots: Vec<_> = self.apps.iter().map(|app| app.snapshot()).collect();
        snapshots.sort_unstable_by(|left, right| left.app_id.cmp(&right.app_id));
        snapshots
    }
}
