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
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_APP_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationInfo {
    pub app_id: String,
    pub user: String,
    pub client_version: Option<String>,
    pub client_git_commit_id: Option<String>,
    pub last_heartbeat: DateTime<Utc>,
    pub registration_time: DateTime<Utc>,
}

impl ApplicationInfo {
    pub fn new(
        app_id: String,
        user: String,
        version: Option<String>,
        git_commit_id: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            app_id,
            user,
            client_version: version,
            client_git_commit_id: git_commit_id,
            last_heartbeat: now,
            registration_time: now,
        }
    }
}

#[derive(Clone)]
pub struct ApplicationManager {
    apps: Arc<DashMap<String, ApplicationInfo>>,
    heartbeat_timeout: Duration,
}

impl Default for ApplicationManager {
    fn default() -> Self {
        Self::new(DEFAULT_APP_HEARTBEAT_TIMEOUT)
    }
}

impl ApplicationManager {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            apps: Arc::new(DashMap::new()),
            heartbeat_timeout,
        }
    }

    pub fn register(
        &self,
        app_id: String,
        user: String,
        version: Option<String>,
        git_commit_id: Option<String>,
    ) {
        self.remove_expired();
        let now = Utc::now();
        self.apps
            .entry(app_id.clone())
            .and_modify(|application| {
                application.user = user.clone();
                application.client_version = version.clone();
                application.client_git_commit_id = git_commit_id.clone();
                application.last_heartbeat = now;
            })
            .or_insert_with(|| {
                info!("Application registered: {app_id} (user: {user})");
                ApplicationInfo::new(app_id, user, version, git_commit_id)
            });
    }

    pub fn heartbeat(&self, app_id: &str) {
        self.remove_expired();
        if let Some(mut application) = self.apps.get_mut(app_id) {
            application.last_heartbeat = Utc::now();
        } else {
            self.register(app_id.to_string(), String::new(), None, None);
        }
    }

    fn remove_expired(&self) {
        let now = Utc::now();
        self.apps.retain(|_, application| {
            now.signed_duration_since(application.last_heartbeat)
                .to_std()
                .map_or(true, |age| age < self.heartbeat_timeout)
        });
    }
}
