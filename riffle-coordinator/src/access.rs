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

use boa_engine::{js_string, module::SimpleModuleLoader, Context, JsValue, Module, Source};
use log::warn;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use crate::grpc::protobuf::uniffle::AccessClusterRequest;

const DEFAULT_ACCESS_SCRIPT: &str = r#"
export function access(_request) {
    return { allow: true };
}
"#;

pub const ACCESS_PLUGIN_PATH_ENV: &str = "RIFFLE_ACCESS_PLUGIN_PATH";

#[derive(Debug)]
pub enum AccessResult {
    Allow,
    Deny(String),
}

#[derive(Debug, thiserror::Error)]
#[error("access plugin failed: {0}")]
pub struct AccessError(pub String);

#[tonic::async_trait]
pub trait AccessPlugin: Send + Sync {
    async fn access(&self, request: &AccessClusterRequest) -> Result<AccessResult, AccessError>;
}

#[derive(Clone)]
enum BoaSource {
    Inline(Arc<str>),
    File(Arc<PathBuf>),
}

#[derive(Clone)]
pub struct BoaAccessPlugin {
    source: BoaSource,
}

impl Default for BoaAccessPlugin {
    fn default() -> Self {
        Self::from_source(DEFAULT_ACCESS_SCRIPT)
    }
}

impl BoaAccessPlugin {
    pub fn from_source(source: impl Into<String>) -> Self {
        let source: Arc<str> = Arc::from(source.into());
        Self {
            source: BoaSource::Inline(source),
        }
    }

    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, AccessError> {
        let path = fs::canonicalize(path.into())
            .map_err(|error| AccessError(format!("failed to locate access plugin: {error}")))?;
        if !path.is_file() {
            return Err(AccessError(format!(
                "access plugin file does not exist: {}",
                path.display()
            )));
        }
        if fs::read_to_string(&path)
            .map_err(|error| AccessError(format!("failed to read access plugin: {error}")))?
            .trim()
            .is_empty()
        {
            return Err(AccessError("access plugin file is empty".to_string()));
        }
        Ok(Self {
            source: BoaSource::File(Arc::new(path)),
        })
    }
}

pub fn load_default_access_plugin() -> Arc<dyn AccessPlugin> {
    let Some(path) = std::env::var_os(ACCESS_PLUGIN_PATH_ENV).filter(|path| !path.is_empty())
    else {
        return Arc::new(BoaAccessPlugin::default());
    };

    match BoaAccessPlugin::from_file(PathBuf::from(path)) {
        Ok(plugin) => Arc::new(plugin),
        Err(error) => {
            warn!(
                "Failed to load access plugin from {ACCESS_PLUGIN_PATH_ENV}: {error}; using allow-all policy"
            );
            Arc::new(AllowAllAccessPlugin)
        }
    }
}

#[tonic::async_trait]
impl AccessPlugin for BoaAccessPlugin {
    async fn access(&self, request: &AccessClusterRequest) -> Result<AccessResult, AccessError> {
        let source = self.source.clone();
        let request = request.clone();

        // ponytail: create one Boa context per request; use a worker pool if policy throughput makes this measurable.
        tokio::task::spawn_blocking(move || evaluate_boa(source, &request))
            .await
            .map_err(|error| AccessError(format!("Boa access task failed: {error}")))?
    }
}

fn evaluate_boa(
    source: BoaSource,
    request: &AccessClusterRequest,
) -> Result<AccessResult, AccessError> {
    let mut context = match &source {
        BoaSource::Inline(_) => Context::default(),
        BoaSource::File(path) => {
            let root = path.parent().unwrap_or_else(|| Path::new("."));
            let loader = Rc::new(SimpleModuleLoader::new(root).map_err(|error| {
                AccessError(format!("failed to create Boa module loader: {error}"))
            })?);
            Context::builder()
                .module_loader(loader)
                .build()
                .map_err(|error| AccessError(format!("failed to create Boa context: {error}")))?
        }
    };

    let module = match &source {
        BoaSource::Inline(source) => {
            Module::parse(Source::from_bytes(source.as_bytes()), None, &mut context)
        }
        BoaSource::File(path) => {
            let source = Source::from_filepath(path).map_err(|error| {
                AccessError(format!(
                    "failed to read access plugin {}: {error}",
                    path.display()
                ))
            })?;
            Module::parse(source, None, &mut context)
        }
    }
    .map_err(|error| AccessError(format!("failed to parse Boa access plugin: {error}")))?;

    module
        .load_link_evaluate(&mut context)
        .await_blocking(&mut context)
        .map_err(|error| AccessError(format!("failed to evaluate Boa access plugin: {error}")))?;

    let access = module
        .get_value(js_string!("access"), &mut context)
        .map_err(|error| AccessError(format!("failed to load access export: {error}")))?;
    let access = access
        .as_callable()
        .ok_or_else(|| AccessError("Boa plugin export `access` must be a function".to_string()))?;

    let input = json!({
        "access_id": request.access_id.clone(),
        "tags": request.tags.clone(),
        "extra_properties": request.extra_properties.clone(),
        "user": request.user.clone(),
    });
    let input = JsValue::from_json(&input, &mut context)
        .map_err(|error| AccessError(format!("failed to convert access request: {error}")))?;
    let result = access
        .call(&JsValue::undefined(), &[input], &mut context)
        .map_err(|error| AccessError(format!("Boa access function failed: {error}")))?;
    let result = result
        .to_json(&mut context)
        .map_err(|error| AccessError(format!("failed to convert access result: {error}")))?;

    parse_access_result(result)
}

fn parse_access_result(result: Option<Value>) -> Result<AccessResult, AccessError> {
    let Some(Value::Object(result)) = result else {
        return Err(AccessError(
            "Boa access function must return an object".to_string(),
        ));
    };
    let allow = result
        .get("allow")
        .and_then(Value::as_bool)
        .ok_or_else(|| AccessError("Boa access result must contain boolean `allow`".to_string()))?;

    if allow {
        Ok(AccessResult::Allow)
    } else {
        Ok(AccessResult::Deny(
            result
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("Access denied by policy")
                .to_string(),
        ))
    }
}

#[derive(Default)]
pub struct AllowAllAccessPlugin;

#[tonic::async_trait]
impl AccessPlugin for AllowAllAccessPlugin {
    async fn access(&self, _request: &AccessClusterRequest) -> Result<AccessResult, AccessError> {
        Ok(AccessResult::Allow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn request(user: &str) -> AccessClusterRequest {
        AccessClusterRequest {
            user: user.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn default_boa_policy_allows_access() {
        let result = BoaAccessPlugin::default()
            .access(&request("any-user"))
            .await
            .unwrap();

        assert!(matches!(result, AccessResult::Allow));
    }

    #[tokio::test]
    async fn boa_source_policy_can_deny_access() {
        let plugin = BoaAccessPlugin::from_source(
            r#"
            export function access(request) {
                return { allow: request.user !== "blocked", reason: "user is blocked" };
            }
            "#,
        );

        let result = plugin.access(&request("blocked")).await.unwrap();

        assert!(matches!(result, AccessResult::Deny(message) if message == "user is blocked"));
    }

    #[tokio::test]
    async fn boa_file_policy_can_import_a_helper_module() {
        let directory = tempdir().unwrap();
        fs::write(
            directory.path().join("helper.js"),
            "export function allowed(user) { return user === 'trusted'; }",
        )
        .unwrap();
        let plugin_path = directory.path().join("access.js");
        fs::write(
            &plugin_path,
            r#"
            import { allowed } from "./helper.js";
            export function access(request) {
                return { allow: allowed(request.user), reason: "user is not trusted" };
            }
            "#,
        )
        .unwrap();

        let plugin = BoaAccessPlugin::from_file(plugin_path).unwrap();
        let allowed = plugin.access(&request("trusted")).await.unwrap();
        let denied = plugin.access(&request("unknown")).await.unwrap();

        assert!(matches!(allowed, AccessResult::Allow));
        assert!(matches!(denied, AccessResult::Deny(message) if message == "user is not trusted"));
    }
}
