use crate::config::Config;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

pub static SERVICE_TAGS_MANAGER_REF: OnceCell<ServiceTagsManager> = OnceCell::new();

pub const LEGACY_VERSION_TAG: &str = "ss_v4";
pub const VERSION_TAG: &str = "ss_v5";
pub const GRPC_TAG: &str = "GRPC";
pub const URPC_TAG: &str = "GRPC_NETTY";

#[derive(Clone)]
pub struct ServiceTagsManager {
    builtin_tags: HashSet<String>,
    tags: Arc<RwLock<HashSet<String>>>,
}

impl ServiceTagsManager {
    pub fn new(config: &Config) -> Self {
        let mut builtin_tags = HashSet::new();
        builtin_tags.insert(String::from(LEGACY_VERSION_TAG));
        builtin_tags.insert(String::from(VERSION_TAG));
        builtin_tags.insert(String::from(GRPC_TAG));
        if config.urpc_port.unwrap_or(0) > 0 {
            builtin_tags.insert(String::from(URPC_TAG));
        }

        let mut tags = HashSet::new();
        tags.extend(config.tags.clone().unwrap_or_default());

        Self {
            builtin_tags,
            tags: Arc::new(RwLock::new(tags)),
        }
    }

    pub fn all_tags(&self) -> Vec<String> {
        let tags = self.tags.read();
        self.builtin_tags
            .iter()
            .chain(tags.iter())
            .cloned()
            .collect()
    }

    pub fn update_tags(&self, new_tags: Vec<String>) {
        let mut tags = self.tags.write();
        tags.clear();
        tags.extend(new_tags);
        info!("Updated tags: {:?}", &*tags);
    }

    pub fn add_tag(&self, tag: String) {
        let mut tags = self.tags.write();
        tags.insert(tag.clone());
        info!("Added tag: {}. Current tags: {:?}", tag, &*tags);
    }

    pub fn delete_tag(&self, tag: String) -> bool {
        if self.builtin_tags.contains(&tag) {
            return false;
        }
        let mut tags = self.tags.write();
        let removed = tags.remove(&tag);
        if removed {
            info!("Deleted tag: {}. Current tags: {:?}", tag, &*tags);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_manager::test::mock_config;

    #[test]
    fn test_builtin_tags_are_not_removable() {
        let config = mock_config();
        let manager = ServiceTagsManager::new(&config);
        assert!(!manager.delete_tag(GRPC_TAG.to_string()));
        assert!(manager.all_tags().contains(&GRPC_TAG.to_string()));
    }

    #[test]
    fn test_update_add_delete_tags() {
        let config = mock_config();
        let manager = ServiceTagsManager::new(&config);

        manager.update_tags(vec!["a1".to_string(), "a2".to_string()]);
        let tags = manager.all_tags();
        assert!(tags.contains(&"a1".to_string()));
        assert!(tags.contains(&"a2".to_string()));

        manager.add_tag("a3".to_string());
        assert!(manager.all_tags().contains(&"a3".to_string()));

        assert!(manager.delete_tag("a1".to_string()));
        assert!(!manager.all_tags().contains(&"a1".to_string()));
    }
}
