use crate::config::Config;
use crate::runtime::{Runtime, RuntimeRef};
use crate::util;
use anyhow::{anyhow, Result};
use bytesize::ByteSize;
use clap::builder::Str;
use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use libc::{if_data, stat};
use log::{error, info, warn};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

fn flatten_json_value(
    prefix: String,
    value: &Value,
    separator: &str,
    map: &mut HashMap<String, Value>,
) {
    match value {
        Value::Object(obj) => {
            for (k, v) in obj {
                let new_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}{}{}", prefix, separator, k)
                };
                flatten_json_value(new_key, v, separator, map);
            }
        }
        Value::Array(arr) => {
            map.insert(prefix.clone(), Value::Array(arr.clone()));
        }
        Value::Null => {
            // skip
        }
        val => {
            map.insert(prefix.clone(), val.clone());
        }
        _ => {}
    }
}

pub static RECONF_MANAGER: OnceLock<ReconfigurableConfManager> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct ReconfigurableConfManager {
    conf_path: String,
    conf_state: Arc<DashMap<String, Value>>,
}

impl ReconfigurableConfManager {
    pub fn new(
        config: &Config,
        conf_path: &str,
        reload_interval: u64,
        rt: &RuntimeRef,
    ) -> Result<ReconfigurableConfManager> {
        let state = Self::to_internal_state(config);
        let manager = ReconfigurableConfManager {
            conf_path: conf_path.to_string(),
            conf_state: Arc::new(state?),
        };

        let manager_fork = manager.clone();
        rt.clone()
            .spawn_with_await_tree("Config reload", async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(reload_interval)).await;
                    if let Err(e) = manager_fork.reload() {
                        error!("Errors on reloading config. err: {:?}", e);
                    }
                }
            });

        Ok(manager)
    }

    pub fn register(&self, key: &str) -> Result<ReconfigValueRef> {
        if !self.conf_state.contains_key(key) {
            panic!("[ReconfigurableConfManager] No such register-key: {}", key)
        }

        info!("Register reconfiguration key for [{}]", key);
        let val = self.conf_state.get(key).unwrap().clone();
        Ok(ReconfigValueRef {
            manager: self.clone(),
            key: key.to_string(),
            previous_value: Mutex::new(val),
        })
    }

    fn reload(&self) -> Result<()> {
        let config_struct = Config::from(&self.conf_path);
        let new_state = Self::to_internal_state(&config_struct)?;

        for (k, v) in new_state {
            if let Some(mut val_ref) = self.conf_state.get_mut(&k) {
                // Only numeric val could be refreshed.
                if *val_ref != v {
                    warn!("Updated [{}] from {:?} to {:?}", &k, &val_ref, &v);
                    *val_ref = v;
                }
            }
        }
        Ok(())
    }

    fn to_internal_state(config: &Config) -> Result<DashMap<String, Value>> {
        let json = serde_json::to_string(config)?;
        let mut state: HashMap<String, Value> = HashMap::new();
        flatten_json_value(
            "".to_string(),
            &serde_json::from_str(&json)?,
            "#",
            &mut state,
        );

        let state: DashMap<String, Value> = state.into_iter().collect();
        Ok(state)
    }
}

pub struct ReconfigValueRef {
    manager: ReconfigurableConfManager,
    key: String,
    previous_value: Mutex<Value>,
}

macro_rules! get_typed_config {
    ($func_name:ident, $value_type:ty, $as_func:ident, $type_name:expr) => {
        pub fn $func_name(&self) -> Result<$value_type, anyhow::Error> {
            match self.manager.conf_state.get(&self.key) {
                Some(val) => match val.$as_func() {
                    Some(v) => {
                        // never reset for previous value. always fallback to the initial value
                        // todo: support resetting in the future
                        // let mut pre_val = self.previous_value.lock();
                        // if (*pre_val != *val) {
                        //     *pre_val = val.clone();
                        // }
                        Ok(v.to_owned())
                    }
                    None => {
                        let prev_val = self.previous_value.lock();
                        if let Some(prev_v) = prev_val.$as_func() {
                            Ok(prev_v.to_owned())
                        } else {
                            Err(anyhow::anyhow!(format!(
                                "Unable to cast previous value into {}. config key: {}",
                                $type_name, &self.key
                            )))
                        }
                    }
                },
                None => {
                    let prev_val = self.previous_value.lock();
                    if let Some(prev_v) = prev_val.$as_func() {
                        Ok(prev_v.to_owned())
                    } else {
                        Err(anyhow::anyhow!(format!(
                            "Unable to cast previous value into {}. config key: {}",
                            $type_name, &self.key
                        )))
                    }
                }
            }
        }
    };
}

impl ReconfigValueRef {
    get_typed_config!(get_i64, i64, as_i64, "i64");
    get_typed_config!(get_f64, f64, as_f64, "f64");
    get_typed_config!(get_u64, u64, as_u64, "u64");
    get_typed_config!(get_str, String, as_str, "str");

    pub fn get_byte_size(&self) -> Result<u64> {
        let raw_byte = self.get_str()?;
        let parsed = raw_byte.parse::<ByteSize>();
        match parsed {
            Ok(parsed) => Ok(parsed.0),
            Err(_) => {
                let prev = self.previous_value.lock();
                println!("prev: {:?}", &prev);
                // ensure the previous value legal
                let prev_str = prev.as_str().ok_or(anyhow!(format!(
                    "Illegal reconfigurable previous key: {}",
                    &self.key
                )))?;
                Ok(util::parse_raw_to_bytesize(prev_str))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::config_reconfigure::{
        flatten_json_value, ReconfigurableConfManager, RECONF_MANAGER,
    };
    use crate::runtime::{Builder, Runtime};
    use anyhow::Result;
    use clap::builder::Str;
    use crossbeam_utils::atomic::AtomicCell;
    use fs2::available_space;
    use libc::sleep;
    use log::info;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tokio::time;

    #[test]
    fn test_atomic_cell() -> Result<()> {
        let cell = AtomicCell::new(0);
        assert_eq!(0, cell.load());

        cell.store(-1);
        assert_eq!(-1, cell.load());
        Ok(())
    }

    fn write_conf_into_file(target_file: String, content: String) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&target_file)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn create_toml_config(memory_spill_high_watermark_ratio: f64) -> String {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024M"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        memory_spill_to_localfile_concurrency = 100
        "#;

        format!(
            "{}\nmemory_spill_high_watermark = {}",
            toml_str, memory_spill_high_watermark_ratio
        )
    }

    #[test]
    fn test_reconf_wrong_but_fallback() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_reconf_wrong_but_fallback").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let toml_str = create_toml_config(0.8);

        let target_conf_file = format!("{}/{}", &temp_path, "conf.file");
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        let config = Config::from(&target_conf_file);
        let runtime = Builder::default()
            .worker_threads(1)
            .thread_name("reload")
            .enable_all()
            .build()?;
        let runtime = Arc::new(runtime);

        let reconf_manager =
            ReconfigurableConfManager::new(&config, &target_conf_file, 1, &runtime)?;

        let reconf_ref_1 = reconf_manager.register("memory_store#capacity")?;
        assert_eq!(1024000000, reconf_ref_1.get_byte_size()?);

        let reconf_ref_2 =
            reconf_manager.register("hybrid_store#memory_spill_to_localfile_concurrency")?;
        assert_eq!(100, reconf_ref_2.get_u64()?);

        // change but wrongly configure
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024WRONG"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        memory_spill_to_localfile_concurrency = -1
        "#;
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        thread::sleep(Duration::from_millis(2000));
        // fallback due to the incorrect conf options
        assert_eq!(100, reconf_ref_2.get_u64()?);
        assert_eq!(1024000000, reconf_ref_1.get_byte_size()?);

        Ok(())
    }

    #[test]
    fn test_reconf() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_reconf_dir").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let toml_str = create_toml_config(0.8);

        let target_conf_file = format!("{}/{}", &temp_path, "conf.file");
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        let config = Config::from(&target_conf_file);
        let runtime = Builder::default()
            .worker_threads(1)
            .thread_name("reload")
            .enable_all()
            .build()?;
        let runtime = Arc::new(runtime);

        let reconf_manager =
            ReconfigurableConfManager::new(&config, &target_conf_file, 1, &runtime)?;

        let reconf_ref_1 = reconf_manager.register("hybrid_store#memory_spill_high_watermark")?;
        assert_eq!(0.8, reconf_ref_1.get_f64()?);

        let reconf_ref_2 = reconf_manager.register("memory_store#capacity")?;
        assert_eq!(1024000000, reconf_ref_2.get_byte_size()?);

        // refresh to 0.2
        let toml_conf = create_toml_config(0.2);
        write_conf_into_file(target_conf_file.to_owned(), toml_conf.to_string())?;

        awaitility::at_most(Duration::from_secs(2))
            .until(|| reconf_ref_1.get_f64().unwrap() == 0.2);

        Ok(())
    }

    #[test]
    fn test_flatten_with_conf() -> Result<()> {
        let raw_json = r#"
            {
                "hybrid_store": {
                    "memory_spill_high_watermark": 0.8,
                    "memory_spill_low_watermark": 0.2,
                    "memory_single_buffer_max_spill_size": "256M",
                    "memory_spill_to_cold_threshold_size": null,
                    "memory_spill_to_localfile_concurrency": null,
                    "memory_spill_to_hdfs_concurrency": null,
                    "huge_partition_memory_spill_to_hdfs_threshold_size": "64M",
                    "sensitive_watermark_spill_enable": false,
                    "async_watermark_spill_trigger_enable": false,
                    "async_watermark_spill_trigger_interval_ms": 500
                },
                "heartbeat_interval_seconds": 2
            }
        "#;

        let mut actual_map: HashMap<String, Value> = HashMap::new();
        flatten_json_value(
            "".to_string(),
            &serde_json::from_str(raw_json)?,
            "#",
            &mut actual_map,
        );
        assert_eq!(actual_map.len(), 8);

        Ok(())
    }

    #[test]
    fn test_flatten() {
        let data = json!({
            "layer_1": "memory",
            "arr_1": [1, 2, 3],
            "null_1": null,
            "additional_config": {
                "nested": {
                    "deep_key": 1.5
                },
                "flat_key": true
            }
        });

        let mut actual_map: HashMap<String, Value> = HashMap::new();
        flatten_json_value("".to_string(), &data, "#", &mut actual_map);

        assert_eq!(
            true,
            actual_map
                .get("additional_config#flat_key")
                .unwrap()
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            1.5,
            actual_map
                .get("additional_config#nested#deep_key")
                .unwrap()
                .as_f64()
                .unwrap()
        );

        assert_eq!(
            3,
            actual_map
                .get("arr_1")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .count()
        );
        assert_eq!(false, actual_map.contains_key("null_1"));
    }
}
