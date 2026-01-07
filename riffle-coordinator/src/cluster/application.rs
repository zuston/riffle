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
use serde::{Deserialize, Serialize};

/// Information about a registered application
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationInfo {
    pub app_id: String,
    pub user: String,
    pub last_heartbeat: DateTime<Utc>,
    pub registration_time: DateTime<Utc>,
}

impl ApplicationInfo {
    pub fn new(app_id: String, user: String) -> Self {
        let now = Utc::now();
        Self {
            app_id,
            user,
            last_heartbeat: now,
            registration_time: now,
        }
    }
}
