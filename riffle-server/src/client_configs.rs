use crate::config::RpcVersion;
use crate::store::local::read_options::IoMode;
use crate::util;
use clap::builder::Str;
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// The configuration options related to riffle-servers on the Uniffle client side.
pub static URPC_READ_IO_MODE_OPTION: Lazy<ClientConfigOption<IoMode>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.riffle.urpcReadIoMode").default_value(IoMode::BUFFER_IO)
});

pub static HDFS_CLIENT_EAGER_LOADING_ENABLED_OPTION: Lazy<ClientConfigOption<bool>> = Lazy::new(
    || {
        ClientConfigOption::key("spark.rss.riffle.hdfsClientEagerLoadingEnabled")
        .default_value(false)
        .with_description("Indicates whether the HDFS client should be eagerly loaded during registration on the Riffle server side.")
    },
);

pub static READ_AHEAD_ENABLED_OPTION: Lazy<ClientConfigOption<bool>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.riffle.readAheadEnabled")
        .default_value(false)
        .with_description("This indicates whether the localfile read ahead is enabled")
});

pub static READ_AHEAD_BATCH_SIZE: Lazy<ClientConfigOption<String>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.riffle.readAheadBatchSize")
        .with_description("Read ahead batch size for client per-read")
});

pub static READ_AHEAD_BATCH_NUMBER: Lazy<ClientConfigOption<usize>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.riffle.readAheadBatchNumber")
        .with_description("Read ahead batch number for client per-read")
});

pub static GET_MEMORY_DATA_URPC_VERSION: Lazy<ClientConfigOption<RpcVersion>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.riffle.getMemoryDataUrpcVersion")
        .default_value(RpcVersion::V1)
        .with_description("the urpc version of getMemoryData")
});

pub static STORAGE_CAPACITY_PARTITION_SPLIT_ENABLED: Lazy<ClientConfigOption<bool>> =
    Lazy::new(|| {
        ClientConfigOption::key("spark.rss.riffle.storageCapacityPartitionSplitEnabled")
            .default_value(false)
            .with_description("whether to trigger partition split by the storage capacity")
    });

#[derive(Debug, Clone, Default)]
pub struct ClientRssConf {
    properties: HashMap<String, String>,
}

impl ClientRssConf {
    pub fn from(properties: HashMap<String, String>) -> Self {
        Self { properties }
    }

    pub fn get<T: std::str::FromStr + Clone + Send + Sync + 'static>(
        &self,
        option: &ClientConfigOption<T>,
    ) -> Option<T> {
        match self.properties.get(&option.key) {
            None => option.default.as_ref().cloned(),
            Some(s) => s.parse::<T>().ok(),
        }
    }

    pub fn get_byte_size(&self, option: &ClientConfigOption<String>) -> Option<u64> {
        self.get(option).map(|s| util::to_bytes(&s))
    }
}

#[derive(Clone, Debug)]
pub struct ClientConfigOption<T: Clone + Send + Sync + 'static> {
    key: String,
    default: Option<T>,
    description: Option<String>,
}

impl<T: Clone + Send + Sync + 'static> ClientConfigOption<T> {
    pub fn key(key: &str) -> Self {
        ClientConfigOption {
            key: key.to_string(),
            default: None,
            description: None,
        }
    }
    pub fn default_value(mut self, value: T) -> Self {
        self.default = Some(value);
        self
    }

    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_get_memory_data_option() {
        // case1: default rpc version is V1
        let mut props = HashMap::new();
        let conf = ClientRssConf { properties: props };
        assert_eq!(
            RpcVersion::V1,
            conf.get(&GET_MEMORY_DATA_URPC_VERSION).unwrap()
        );

        // case2: implicit setting rpc version
        let mut props = HashMap::new();
        props.insert(
            "spark.rss.riffle.getMemoryDataUrpcVersion".to_string(),
            "V2".to_string(),
        );
        let conf = ClientRssConf { properties: props };
        assert_eq!(
            RpcVersion::V2,
            conf.get(&GET_MEMORY_DATA_URPC_VERSION).unwrap()
        );

        // case3: fallback to v1 version when version is illegal
        let mut props = HashMap::new();
        props.insert(
            "spark.rss.riffle.getMemoryDataUrpcVersion".to_string(),
            "V12323".to_string(),
        );
        let conf = ClientRssConf { properties: props };
        assert_eq!(
            RpcVersion::V1,
            conf.get(&GET_MEMORY_DATA_URPC_VERSION).unwrap()
        );
    }

    #[test]
    fn test_byte_size() {
        let mut props = HashMap::new();
        props.insert(
            "spark.rss.riffle.readAheadBatchSize".to_string(),
            "14M".to_string(),
        );
        let conf = ClientRssConf { properties: props };
        assert_eq!(
            bytesize::MB * 14,
            conf.get_byte_size(&READ_AHEAD_BATCH_SIZE).unwrap()
        );
    }

    #[test]
    fn test_no_default_value() {
        let conf = ClientRssConf::default();
        assert_eq!(None, conf.get(&READ_AHEAD_BATCH_SIZE));
    }
}
