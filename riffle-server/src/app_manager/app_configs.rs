use crate::app_manager::request_context::PurgeDataContext;
use crate::client_configs::{
    ClientConfigOption, ClientRssConf, GET_MEMORY_DATA_URPC_VERSION,
    HDFS_CLIENT_EAGER_LOADING_ENABLED_OPTION, READ_AHEAD_BATCH_NUMBER, READ_AHEAD_BATCH_SIZE,
    READ_AHEAD_ENABLED_OPTION, URPC_READ_IO_MODE_OPTION,
};
use crate::config::RpcVersion;
use crate::grpc::protobuf::uniffle::RemoteStorage;
use crate::store::local::read_options::IoMode;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use strum_macros::Display;

pub const MAX_CONCURRENCY_PER_PARTITION_TO_WRITE: i32 = 20;

#[derive(Debug, Clone, Display)]
pub enum DataDistribution {
    NORMAL,
    #[allow(non_camel_case_types)]
    LOCAL_ORDER,
}

#[derive(Debug, Clone)]
pub struct AppConfigOptions {
    pub data_distribution: DataDistribution,
    pub max_concurrency_per_partition_to_write: i32,
    pub remote_storage_config_option: Option<RemoteStorageConfig>,

    // about read ahead configs
    pub read_ahead_enable: bool,
    pub read_ahead_batch_number: Option<usize>,
    pub read_ahead_batch_size: Option<usize>,

    // the urpc endpoint version
    pub get_memory_data_urpc_version: RpcVersion,

    // urpc_read_io_mode
    pub urpc_read_io_mode: IoMode,

    // all client configs
    pub client_configs: ClientRssConf,
}

impl AppConfigOptions {
    pub fn new(
        data_distribution: DataDistribution,
        max_concurrency_per_partition_to_write: i32,
        remote_storage_config_option: Option<RemoteStorageConfig>,
        rss_config: ClientRssConf,
    ) -> Self {
        Self {
            data_distribution,
            max_concurrency_per_partition_to_write,
            remote_storage_config_option,
            read_ahead_enable: rss_config.get(&READ_AHEAD_ENABLED_OPTION).unwrap_or(false),
            read_ahead_batch_number: rss_config.get(&READ_AHEAD_BATCH_NUMBER),
            read_ahead_batch_size: rss_config
                .get_byte_size(&READ_AHEAD_BATCH_SIZE)
                .map(|x| x as usize),
            get_memory_data_urpc_version: rss_config
                .get(&GET_MEMORY_DATA_URPC_VERSION)
                .unwrap_or(RpcVersion::V1),
            urpc_read_io_mode: rss_config
                .get(&URPC_READ_IO_MODE_OPTION)
                .unwrap_or(IoMode::BUFFER_IO),
            client_configs: rss_config,
        }
    }
}

impl Default for AppConfigOptions {
    fn default() -> Self {
        AppConfigOptions {
            data_distribution: DataDistribution::LOCAL_ORDER,
            max_concurrency_per_partition_to_write: 20,
            remote_storage_config_option: None,
            read_ahead_enable: false,
            read_ahead_batch_number: None,
            read_ahead_batch_size: None,
            client_configs: Default::default(),
            get_memory_data_urpc_version: RpcVersion::V1,
            urpc_read_io_mode: Default::default(),
        }
    }
}

impl Display for AppConfigOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "data_distribution={}, urpc_read_io_mode={}, read_ahead_enable={}",
            &self.data_distribution, self.urpc_read_io_mode, self.read_ahead_enable
        )
    }
}

// =============================================================

#[derive(Clone, Debug)]
pub struct RemoteStorageConfig {
    pub root: String,
    pub configs: HashMap<String, String>,
}

impl From<RemoteStorage> for RemoteStorageConfig {
    fn from(remote_conf: RemoteStorage) -> Self {
        let root = remote_conf.path;
        let mut confs = HashMap::new();
        for kv in remote_conf.remote_storage_conf {
            confs.insert(kv.key, kv.value);
        }

        Self {
            root,
            configs: confs,
        }
    }
}

impl fmt::Display for RemoteStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "RemoteStorageConfig:")?;
        writeln!(f, "  Root: {}", self.root)?;
        writeln!(f, "  Configs:")?;
        for (key, value) in &self.configs {
            writeln!(f, "    {}: {}", key, value)?;
        }
        Ok(())
    }
}
