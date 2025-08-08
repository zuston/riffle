use log::{debug, error, info};
use riffle_server::config::Config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[macro_export]
macro_rules! unwrap_or_return_empty_map {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                debug!("err: {}", e);
                return ::std::collections::HashMap::new();
            }
        }
    };
}

fn default_meta_file_path() -> String {
    let path = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".riffle")
        .join("meta.file");
    path.to_str().unwrap().to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct ClusterConfigs {
    clusters: Vec<ClusterId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct ClusterId {
    cluster_id: String,
    coordinator_url: String,
}

pub struct Metadata {
    meta_file_path: String,
}

impl Metadata {
    // Load the config file from the meta file
    pub fn load(meta_path: Option<&str>) -> Self {
        let meta_path = if let Some(p) = meta_path {
            p
        } else {
            &default_meta_file_path()
        };

        Self {
            meta_file_path: meta_path.into(),
        }
    }

    // Only valid for initialization
    pub fn create(meta_path: Option<&str>, config_path: &str) -> Self {
        let meta_path = if let Some(p) = meta_path {
            p
        } else {
            &default_meta_file_path()
        };
        info!("meta file path: {}", meta_path);

        let parent_dir = Path::new(meta_path).parent().unwrap();
        fs::create_dir_all(parent_dir).unwrap();

        let content = fs::read_to_string(config_path).expect("Failed to read config file");
        let _: ClusterConfigs = toml::from_str(&content).expect("Invalid config file");

        // Store the config file path into meta file
        fs::write(meta_path, config_path).expect("Failed to write meta file");

        Self {
            meta_file_path: meta_path.into(),
        }
    }

    pub fn get_cluster_ids(&self) -> HashMap<String, String> {
        let config_path = unwrap_or_return_empty_map!(fs::read_to_string(&self.meta_file_path));

        let content = unwrap_or_return_empty_map!(fs::read_to_string(config_path.trim()));
        let config: ClusterConfigs = unwrap_or_return_empty_map!(toml::from_str(&content));

        config
            .clusters
            .iter()
            .map(|c| (c.cluster_id.clone(), c.coordinator_url.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_empty() {
        let dir = tempdir().unwrap();
        let meta_path = dir.path().join("meta.file");

        let meta = Metadata::load(meta_path.to_str());
        assert_eq!(0, meta.get_cluster_ids().len());
    }

    #[test]
    fn test_create_and_get_cluster_ids() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("test_config.toml");
        let meta_path = dir.path().join("meta.file");

        let config_content = r#"
            [[clusters]]
            cluster_id = "abc"
            coordinator_url = "http://localhost:12345"
        "#;
        fs::write(&config_path, config_content).unwrap();

        let metadata = Metadata::create(
            Some(meta_path.to_str().unwrap()),
            config_path.to_str().unwrap(),
        );

        assert_eq!(metadata.meta_file_path, meta_path.to_str().unwrap());

        fs::write(&meta_path, config_path.to_str().unwrap()).unwrap();

        let metadata2 = Metadata {
            meta_file_path: meta_path.to_str().unwrap().to_string(),
        };

        let clusters = metadata2.get_cluster_ids();
        assert_eq!(clusters.get("abc").unwrap(), "http://localhost:12345");
    }
}
