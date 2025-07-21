use crate::client_configs::{
    ClientConfigOption, ClientRssConf, HDFS_CLIENT_EAGER_LOADING_ENABLED_OPTION,
    SENDFILE_ENABLED_OPTION,
};
use crate::grpc::protobuf::uniffle::RemoteStorage;
use std::collections::HashMap;
use std::fmt;

pub const MAX_CONCURRENCY_PER_PARTITION_TO_WRITE: i32 = 20;

#[derive(Debug, Clone)]
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
    pub sendfile_enable: bool,
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
            sendfile_enable: rss_config.get(&SENDFILE_ENABLED_OPTION).unwrap_or(false),
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
            sendfile_enable: false,
            client_configs: Default::default(),
        }
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
