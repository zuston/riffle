use crate::config_reconfigure::ReconfigurableConfManager;
use crate::util;
use bytesize::ByteSize;
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

pub type ConfigOption<T> = Box<dyn ConfRef<T, Output = T>>;

/// The config_ref is to wrap the dynamic value retrieved by the specified key

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ByteString {
    pub val: String,
    pub parsed_val: u64,
}

unsafe impl Send for ByteString {}
unsafe impl Sync for ByteString {}

impl ByteString {
    pub fn as_u64(&self) -> u64 {
        self.parsed_val
    }
}

impl Display for ByteString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.val)
    }
}

impl<'de> Deserialize<'de> for ByteString {
    fn deserialize<D>(deserializer: D) -> anyhow::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let val = raw.parse::<ByteSize>().map_err(serde::de::Error::custom)?.0;
        Ok(ByteString {
            val: raw,
            parsed_val: val,
        })
    }
}

impl Into<u64> for ByteString {
    fn into(self) -> u64 {
        util::parse_raw_to_bytesize(&self.val)
    }
}

// =======================================================

pub trait ConfRef<T: Clone + Send + Sync + 'static>: Send + Sync {
    type Output;
    fn get(&self) -> Self::Output;
}

pub struct DynamicConfRef<T> {
    // todo: remove this public modifier for better safety
    pub manager: ReconfigurableConfManager,
    pub key: String,
    pub value: RwLock<T>,
    pub last_update_timestamp: AtomicU64,
    pub refresh_interval: u64,
    pub lock: Mutex<()>,
}

impl<T> DynamicConfRef<T>
where
    T: DeserializeOwned + Clone,
{
    pub fn new(
        manager: &ReconfigurableConfManager,
        key: &str,
        initial_value: T,
        refresh_interval: u64,
    ) -> Self {
        Self {
            manager: manager.clone(),
            key: key.to_string(),
            value: RwLock::new(initial_value),
            last_update_timestamp: AtomicU64::new(util::now_timestamp_as_sec()),
            refresh_interval,
            lock: Default::default(),
        }
    }
}

impl<T> ConfRef<T> for DynamicConfRef<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Output = T;

    fn get(&self) -> T {
        if self.lock.try_lock().is_some() {
            let now_sec = util::now_timestamp_as_sec();
            let last = self.last_update_timestamp.load(Ordering::Relaxed);
            if now_sec - last > self.refresh_interval {
                if let Some(val) = self.manager.conf_state.get(&self.key) {
                    if let Ok(val) = serde_json::from_value::<T>(val.clone()) {
                        let mut internal_val = self.value.write();
                        *internal_val = val;
                    }
                }
                self.last_update_timestamp.store(now_sec, Ordering::Relaxed);
            }
        }
        let val = self.value.read();
        val.clone()
    }
}

// =======================================================

pub struct FixedConfRef<T> {
    val: T,
}

impl<T> FixedConfRef<T> {
    pub fn new(val: T) -> Self {
        Self { val }
    }
}

impl<T> ConfRef<T> for FixedConfRef<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Output = T;

    fn get(&self) -> Self::Output {
        self.val.clone()
    }
}

#[cfg(test)]
mod test {
    use crate::config_ref::{ConfRef, FixedConfRef};

    #[test]
    fn test_fixed_conf_ref() -> anyhow::Result<()> {
        let const_val = 42;
        let conf_ref = FixedConfRef::new(const_val);
        assert_eq!(conf_ref.get(), const_val);
        Ok(())
    }
}
