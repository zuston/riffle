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

use crate::block_id_manager::BlockIdManagerType;
use crate::store::ResponseDataIndex::Local;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryStoreConfig {
    pub capacity: String,

    #[serde(default = "as_default_buffer_ticket_timeout_sec")]
    pub buffer_ticket_timeout_sec: i64,
    #[serde(default = "as_default_buffer_ticket_timeout_check_interval_sec")]
    pub buffer_ticket_check_interval_sec: i64,
}

fn as_default_buffer_ticket_timeout_check_interval_sec() -> i64 {
    10
}

fn as_default_buffer_ticket_timeout_sec() -> i64 {
    5 * 60
}

impl MemoryStoreConfig {
    pub fn new(capacity: String) -> Self {
        Self {
            capacity,
            buffer_ticket_timeout_sec: as_default_buffer_ticket_timeout_sec(),
            buffer_ticket_check_interval_sec: as_default_buffer_ticket_timeout_check_interval_sec(),
        }
    }

    pub fn from(capacity: String, buffer_ticket_timeout_sec: i64) -> Self {
        Self {
            capacity,
            buffer_ticket_timeout_sec,
            buffer_ticket_check_interval_sec: as_default_buffer_ticket_timeout_check_interval_sec(),
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HdfsStoreConfig {
    #[serde(default = "as_default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "as_default_partition_write_max_concurrency")]
    pub partition_write_max_concurrency: usize,

    pub kerberos_security_config: Option<KerberosSecurityConfig>,

    #[serde(default = "bool::default")]
    pub precheck_enable: bool,
}
fn as_default_max_concurrency() -> usize {
    50
}
fn as_default_partition_write_max_concurrency() -> usize {
    20
}

impl Default for HdfsStoreConfig {
    fn default() -> Self {
        Self {
            max_concurrency: as_default_max_concurrency(),
            partition_write_max_concurrency: as_default_partition_write_max_concurrency(),
            kerberos_security_config: None,
            precheck_enable: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct KerberosSecurityConfig {
    pub keytab_path: String,
    pub principal: String,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LocalfileStoreConfig {
    pub data_paths: Vec<String>,
    pub min_number_of_available_disks: Option<i32>,

    #[serde(default = "bool::default")]
    pub launch_purge_enable: bool,

    #[serde(default = "as_default_disk_high_watermark")]
    pub disk_high_watermark: f32,
    #[serde(default = "as_default_disk_low_watermark")]
    pub disk_low_watermark: f32,
    #[serde(default = "as_default_disk_write_buf_capacity")]
    pub disk_write_buf_capacity: String,
    #[serde(default = "as_default_disk_read_buf_capacity")]
    pub disk_read_buf_capacity: String,
    #[serde(default = "as_default_disk_healthy_check_interval_sec")]
    pub disk_healthy_check_interval_sec: u64,

    #[serde(default = "as_default_direct_io_enable")]
    pub direct_io_enable: bool,
    #[serde(default = "as_default_direct_io_read_enable")]
    pub direct_io_read_enable: bool,
    #[serde(default = "as_default_direct_io_append_enable")]
    pub direct_io_append_enable: bool,

    #[serde(default = "as_default_io_duration_threshold_sec")]
    pub io_duration_threshold_sec: usize,

    // default is false!
    #[serde(default = "bool::default")]
    pub index_consistency_detection_enable: bool,

    pub io_limiter: Option<IoLimiterConfig>,

    // This is only for urpc
    #[serde(default = "as_default_read_io_sendfile_enable")]
    pub read_io_sendfile_enable: bool,

    pub read_ahead_options: Option<ReadAheadConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReadAheadConfig {
    #[serde(default = "as_default_read_ahead_batch_size")]
    pub batch_size: String,
    #[serde(default = "as_default_read_ahead_batch_number")]
    pub batch_number: usize,

    #[serde(default = "bool::default")]
    pub read_plan_enable: bool,
    #[serde(default = "as_default_read_plan_concurrency")]
    pub read_plan_concurrency: usize,
}

fn as_default_read_plan_concurrency() -> usize {
    100
}

impl Default for ReadAheadConfig {
    fn default() -> Self {
        Self {
            batch_size: as_default_read_ahead_batch_size(),
            batch_number: as_default_read_ahead_batch_number(),
            read_plan_enable: false,
            read_plan_concurrency: as_default_read_plan_concurrency(),
        }
    }
}

fn as_default_read_ahead_batch_number() -> usize {
    4
}

fn as_default_read_ahead_batch_size() -> String {
    "14M".to_string()
}

fn as_default_read_io_sendfile_enable() -> bool {
    false
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IoLimiterConfig {
    pub capacity: String,
    #[serde(default = "as_default_io_limit_read_enable")]
    pub read_enable: bool,
    #[serde(default = "as_default_io_limit_write_enable")]
    pub write_enable: bool,
}

fn as_default_io_limit_read_enable() -> bool {
    true
}
fn as_default_io_limit_write_enable() -> bool {
    true
}

impl Default for LocalfileStoreConfig {
    fn default() -> Self {
        LocalfileStoreConfig::new(Vec::new())
    }
}

fn as_default_io_duration_threshold_sec() -> usize {
    5 * 60
}

fn as_default_direct_io_enable() -> bool {
    false
}

fn as_default_direct_io_read_enable() -> bool {
    true
}
fn as_default_direct_io_append_enable() -> bool {
    true
}

fn as_default_disk_healthy_check_interval_sec() -> u64 {
    60
}
fn as_default_disk_low_watermark() -> f32 {
    0.7
}
fn as_default_disk_high_watermark() -> f32 {
    0.8
}
fn as_default_disk_write_buf_capacity() -> String {
    "1M".to_string()
}
fn as_default_disk_read_buf_capacity() -> String {
    "1M".to_string()
}

impl LocalfileStoreConfig {
    pub fn new(data_paths: Vec<String>) -> Self {
        LocalfileStoreConfig {
            data_paths,
            min_number_of_available_disks: Some(1),
            launch_purge_enable: false,
            disk_high_watermark: as_default_disk_high_watermark(),
            disk_low_watermark: as_default_disk_low_watermark(),
            disk_write_buf_capacity: as_default_disk_write_buf_capacity(),
            disk_read_buf_capacity: as_default_disk_read_buf_capacity(),
            disk_healthy_check_interval_sec: as_default_disk_healthy_check_interval_sec(),
            direct_io_enable: as_default_direct_io_enable(),
            direct_io_read_enable: as_default_direct_io_read_enable(),
            direct_io_append_enable: as_default_direct_io_append_enable(),
            io_duration_threshold_sec: as_default_io_duration_threshold_sec(),
            index_consistency_detection_enable: false,
            io_limiter: None,
            read_io_sendfile_enable: as_default_read_io_sendfile_enable(),
            read_ahead_options: None,
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RuntimeConfig {
    pub read_thread_num: usize,
    pub localfile_write_thread_num: usize,
    pub hdfs_write_thread_num: usize,
    pub http_thread_num: usize,
    pub default_thread_num: usize,
    pub dispatch_thread_num: usize,
    pub read_ahead_thread_number: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            read_thread_num: 100,
            localfile_write_thread_num: 100,
            hdfs_write_thread_num: 20,
            http_thread_num: 2,
            default_thread_num: 10,
            dispatch_thread_num: 100,
            read_ahead_thread_number: 100,
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct HealthServiceConfig {
    pub alive_app_number_max_limit: Option<usize>,
    pub disk_used_ratio_health_threshold: Option<f64>,
    // the threshold of the memory allocated from allocator
    pub memory_allocated_threshold: Option<String>,

    pub service_hang_of_mem_continuous_unchange_sec: Option<usize>,
    pub service_hang_of_app_valid_number: Option<usize>,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HybridStoreConfig {
    #[serde(default = "as_default_memory_spill_high_watermark")]
    pub memory_spill_high_watermark: f32,
    #[serde(default = "as_default_memory_spill_low_watermark")]
    pub memory_spill_low_watermark: f32,

    pub memory_single_buffer_max_spill_size: Option<String>,
    pub memory_spill_to_cold_threshold_size: Option<String>,

    pub memory_spill_to_localfile_concurrency: Option<i32>,
    pub memory_spill_to_hdfs_concurrency: Option<i32>,

    pub huge_partition_memory_spill_to_hdfs_threshold_size: Option<String>,
    #[serde(default = "as_default_huge_partition_fallback_enable")]
    pub huge_partition_fallback_enable: bool,

    #[serde(default = "as_default_sensitive_watermark_spill_enable")]
    pub sensitive_watermark_spill_enable: bool,

    #[serde(default = "as_default_async_watermark_spill_trigger_enable")]
    pub async_watermark_spill_trigger_enable: bool,
    #[serde(default = "as_default_async_watermark_spill_trigger_interval_ms")]
    pub async_watermark_spill_trigger_interval_ms: u64,
}

fn as_default_huge_partition_fallback_enable() -> bool {
    false
}

fn as_default_async_watermark_spill_trigger_interval_ms() -> u64 {
    500
}

fn as_default_async_watermark_spill_trigger_enable() -> bool {
    false
}

fn as_default_sensitive_watermark_spill_enable() -> bool {
    false
}

fn as_default_memory_spill_to_localfile_concurrency() -> i32 {
    4000
}
fn as_default_memory_spill_to_hdfs_concurrency() -> i32 {
    500
}
fn as_default_memory_spill_high_watermark() -> f32 {
    0.8
}
fn as_default_memory_spill_low_watermark() -> f32 {
    0.2
}

impl HybridStoreConfig {
    pub fn new(
        memory_spill_high_watermark: f32,
        memory_spill_low_watermark: f32,
        memory_single_buffer_max_spill_size: Option<String>,
    ) -> Self {
        HybridStoreConfig {
            memory_spill_high_watermark,
            memory_spill_low_watermark,
            memory_single_buffer_max_spill_size,
            memory_spill_to_cold_threshold_size: None,
            memory_spill_to_localfile_concurrency: None,
            memory_spill_to_hdfs_concurrency: None,
            huge_partition_memory_spill_to_hdfs_threshold_size: None,
            huge_partition_fallback_enable: as_default_huge_partition_fallback_enable(),
            sensitive_watermark_spill_enable: as_default_sensitive_watermark_spill_enable(),
            async_watermark_spill_trigger_enable: as_default_async_watermark_spill_trigger_enable(),
            async_watermark_spill_trigger_interval_ms:
                as_default_async_watermark_spill_trigger_interval_ms(),
        }
    }
}

impl Default for HybridStoreConfig {
    fn default() -> Self {
        HybridStoreConfig {
            memory_spill_high_watermark: as_default_memory_spill_high_watermark(),
            memory_spill_low_watermark: as_default_memory_spill_low_watermark(),
            memory_single_buffer_max_spill_size: None,
            memory_spill_to_cold_threshold_size: None,
            memory_spill_to_localfile_concurrency: None,
            memory_spill_to_hdfs_concurrency: None,
            huge_partition_memory_spill_to_hdfs_threshold_size: None,
            huge_partition_fallback_enable: as_default_huge_partition_fallback_enable(),
            sensitive_watermark_spill_enable: as_default_sensitive_watermark_spill_enable(),
            async_watermark_spill_trigger_enable: as_default_async_watermark_spill_trigger_enable(),
            async_watermark_spill_trigger_interval_ms:
                as_default_async_watermark_spill_trigger_interval_ms(),
        }
    }
}

fn as_default_runtime_config() -> RuntimeConfig {
    RuntimeConfig::default()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HistoricalAppStoreConfig {
    pub retention_days: usize,
    pub backend: HistoricalAppStoreBackend,
    pub db_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum HistoricalAppStoreBackend {
    MEM,
    SLED,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Config {
    #[serde(default = "as_default_hybrid_store_config")]
    pub hybrid_store: HybridStoreConfig,

    pub memory_store: Option<MemoryStoreConfig>,
    pub localfile_store: Option<LocalfileStoreConfig>,
    pub hdfs_store: Option<HdfsStoreConfig>,

    #[serde(default = "as_default_storage_type")]
    pub store_type: StorageType,

    #[serde(default = "as_default_runtime_config")]
    pub runtime_config: RuntimeConfig,

    pub metrics: Option<MetricsConfig>,

    #[serde(default = "as_default_grpc_port")]
    pub grpc_port: u16,
    pub urpc_port: Option<u16>,
    #[serde(default = "as_default_http_port")]
    pub http_port: u16,

    pub coordinator_quorum: Vec<String>,
    pub tags: Option<Vec<String>>,

    pub log: Option<LogConfig>,

    #[serde(default = "as_default_app_config")]
    pub app_config: AppConfig,

    pub tracing: Option<TracingConfig>,

    #[serde(default = "as_default_health_service_config")]
    pub health_service_config: HealthServiceConfig,

    #[serde(default = "as_default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u32,

    #[serde(default = "as_default_fallback_random_ports_enable")]
    pub fallback_random_ports_enable: bool,

    #[serde(default = "as_default_initial_unhealthy_status_enable")]
    pub initial_unhealthy_status_enable: bool,

    pub historical_apps_config: Option<HistoricalAppStoreConfig>,

    #[serde(default = "as_default_conf_reload_enable")]
    pub conf_reload_enable: bool,

    pub urpc_config: Option<UrpcConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UrpcConfig {
    pub get_index_rpc_version: RpcVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RpcVersion {
    V1,
    V2,
    V3,
}

impl Default for RpcVersion {
    fn default() -> Self {
        RpcVersion::V1
    }
}

fn as_default_conf_reload_enable() -> bool {
    true
}

fn as_default_initial_unhealthy_status_enable() -> bool {
    false
}

fn as_default_fallback_random_ports_enable() -> bool {
    true
}

// ====
fn as_default_heartbeat_interval_seconds() -> u32 {
    2
}
fn as_default_health_service_config() -> HealthServiceConfig {
    Default::default()
}
fn as_default_hybrid_store_config() -> HybridStoreConfig {
    HybridStoreConfig::default()
}
fn as_default_http_port() -> u16 {
    20010
}

fn as_default_storage_type() -> StorageType {
    StorageType::MEMORY
}

fn as_default_grpc_port() -> u16 {
    19999
}

// ===========

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppConfig {
    #[serde(default = "as_default_app_heartbeat_timeout_min")]
    pub app_heartbeat_timeout_min: u32,

    // for the partition limit mechanism
    #[serde(default = "bool::default")]
    pub partition_limit_enable: bool,

    #[serde(default = "as_default_partition_limit_threshold")]
    pub partition_limit_threshold: String,

    #[serde(default = "as_default_partition_limit_memory_backpressure_ratio")]
    pub partition_limit_memory_backpressure_ratio: f64,

    #[serde(default = "as_default_block_id_manager_type")]
    pub block_id_manager_type: BlockIdManagerType,

    // for the partition split mechanism
    #[serde(default = "bool::default")]
    pub partition_split_enable: bool,

    #[serde(default = "as_default_partition_split_threshold")]
    pub partition_split_threshold: String,
}

fn as_default_partition_limit_memory_backpressure_ratio() -> f64 {
    0.2
}

fn as_default_partition_limit_threshold() -> String {
    "20G".to_owned()
}
fn as_default_partition_limit_enable() -> bool {
    true
}

impl Default for AppConfig {
    fn default() -> Self {
        as_default_app_config()
    }
}

fn as_default_partition_split_threshold() -> String {
    "40G".to_owned()
}

fn as_default_block_id_manager_type() -> BlockIdManagerType {
    BlockIdManagerType::DEFAULT
}

fn as_default_app_config() -> AppConfig {
    AppConfig {
        app_heartbeat_timeout_min: as_default_app_heartbeat_timeout_min(),
        partition_limit_enable: false,
        partition_limit_threshold: as_default_partition_limit_threshold(),
        partition_limit_memory_backpressure_ratio:
            as_default_partition_limit_memory_backpressure_ratio(),
        block_id_manager_type: as_default_block_id_manager_type(),
        partition_split_enable: false,
        partition_split_threshold: as_default_partition_split_threshold(),
    }
}

fn as_default_app_heartbeat_timeout_min() -> u32 {
    5
}

// =========================================================
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TracingConfig {
    pub jaeger_reporter_endpoint: String,
    pub jaeger_service_name: String,
}

// =========================================================
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetricsConfig {
    pub push_gateway_endpoint: Option<String>,

    #[serde(default = "as_default_push_interval_sec")]
    pub push_interval_sec: u32,

    pub labels: Option<HashMap<String, String>>,
}

fn as_default_push_interval_sec() -> u32 {
    10
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogConfig {
    pub path: String,
    #[serde(default = "as_default_rotation_config")]
    pub rotation: RotationConfig,

    #[serde(default = "as_default_max_file_size")]
    pub max_file_size: String,
    #[serde(default = "as_default_max_log_files")]
    pub max_log_files: usize,
    #[serde(default = "as_default_log_level")]
    pub log_level: LogLevel,
}

fn as_default_log_level() -> LogLevel {
    LogLevel::INFO
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    DEBUG,
    INFO,
    WARN,
}

fn as_default_max_file_size() -> String {
    "512M".to_string()
}

fn as_default_max_log_files() -> usize {
    10
}

fn as_default_rotation_config() -> RotationConfig {
    RotationConfig::Daily
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            path: "/tmp/".to_string(),
            rotation: RotationConfig::Hourly,
            max_file_size: as_default_max_file_size(),
            max_log_files: as_default_max_log_files(),
            log_level: as_default_log_level(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RotationConfig {
    Hourly,
    Daily,
    Never,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Copy, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum StorageType {
    MEMORY = 1,
    LOCALFILE = 2,
    MEMORY_LOCALFILE = 3,
    HDFS = 4,
    MEMORY_HDFS = 5,
    MEMORY_LOCALFILE_HDFS = 7,
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::MEMORY
    }
}

impl StorageType {
    pub fn contains_localfile(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::LOCALFILE as u8 != 0
    }

    pub fn contains_memory(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::MEMORY as u8 != 0
    }

    pub fn contains_hdfs(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::HDFS as u8 != 0
    }
}

const CONFIG_FILE_PATH_KEY: &str = "WORKER_CONFIG_PATH";

impl Config {
    pub fn from(cfg_path: &str) -> Self {
        let path = Path::new(cfg_path);

        // Read the file content as a string
        let file_content = fs::read_to_string(path).expect("Failed to read file");

        toml::from_str(&file_content).unwrap()
    }

    pub fn create_from_env() -> Config {
        let path = match std::env::var(CONFIG_FILE_PATH_KEY) {
            Ok(val) => val,
            _ => panic!(
                "config path must be set in env args. key: {}",
                CONFIG_FILE_PATH_KEY
            ),
        };

        Config::from(&path)
    }

    pub fn create_mem_localfile_config(
        grpc_port: i32,
        capacity: String,
        local_data_path: String,
    ) -> Config {
        let toml_str = format!(
            r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = [""]
        grpc_port = {:?}

        [memory_store]
        capacity = {:?}

        [localfile_store]
        data_paths = [{:?}]
        "#,
            grpc_port, capacity, local_data_path
        );

        toml::from_str(toml_str.as_str()).unwrap()
    }

    pub fn create_simple_config() -> Config {
        let toml_str = r#"
        store_type = "MEMORY"
        coordinator_quorum = [""]
        grpc_port = 19999

        [memory_store]
        capacity = "1M"

        [hybrid_store]
        memory_spill_high_watermark = 0.8
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        "#;

        toml::from_str(toml_str).unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::config::{
        as_default_app_heartbeat_timeout_min, Config, RpcVersion, RuntimeConfig, StorageType,
    };
    use crate::readable_size::ReadableSize;
    use std::str::FromStr;

    #[test]
    fn storage_type_test() {
        let stype = StorageType::MEMORY_LOCALFILE;
        assert_eq!(true, StorageType::contains_localfile(&stype));

        let stype = StorageType::MEMORY_LOCALFILE;
        assert_eq!(true, StorageType::contains_memory(&stype));
        assert_eq!(false, StorageType::contains_hdfs(&stype));

        let stype = StorageType::MEMORY_LOCALFILE_HDFS;
        assert_eq!(true, StorageType::contains_hdfs(&stype));
    }

    #[test]
    fn config_create() {
        let config =
            Config::create_mem_localfile_config(100, "20g".to_string(), "/tmp/a".to_string());
        println!("{:#?}", config);
    }

    #[test]
    fn config_test() {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [urpc_config]
        get_index_rpc_version = "V2"

        [memory_store]
        capacity = "1024M"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_high_watermark = 0.8
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"

        [hdfs_store]
        max_concurrency = 10

        [hdfs_store.kerberos_security_config]
        keytab_path = "/tmp/a.keytab"
        principal = "a@xxx"

        [metrics]
        push_gateway_endpoint = "http://localhost:5000"
        labels = { l1 = "k1", l2 = "k2" }
        "#;

        let decoded: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", decoded);

        let capacity = ReadableSize::from_str(&decoded.memory_store.unwrap().capacity).unwrap();
        assert_eq!(1024 * 1024 * 1024, capacity.as_bytes());

        assert_eq!(
            decoded.runtime_config.read_thread_num,
            RuntimeConfig::default().read_thread_num
        );

        assert_eq!(
            RpcVersion::V2,
            decoded.urpc_config.as_ref().unwrap().get_index_rpc_version
        );

        // check the app config
        assert_eq!(
            decoded.app_config.app_heartbeat_timeout_min,
            as_default_app_heartbeat_timeout_min(),
        );

        // check kerberos config
        let hdfs = decoded.hdfs_store.unwrap();
        let kerberos_config = hdfs.kerberos_security_config.unwrap();
        assert_eq!(kerberos_config.principal, "a@xxx");

        // check labels of metrics
        let metrics_labels = decoded.metrics.unwrap().labels;
        assert_eq!(2, metrics_labels.unwrap().len());
    }
}
