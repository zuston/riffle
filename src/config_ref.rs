use crate::config_reconfigure::ReconfigurableConfManager;
use crate::util;
use bytesize::ByteSize;
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type ConfigOption<T> = Arc<dyn ConfRef<T, Output = T>>;

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

// Using a trait object for type erasure to make trait impls could be stored
// into a container
pub trait ConfigOptionWrapper: Send + Sync {
    fn update(&self, value: &Value);
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T> ConfigOptionWrapper for ConfigOption<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn update(&self, value: &Value) {
        self.on_change(value);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub trait ConfRef<T: Clone + Send + Sync + 'static>: Send + Sync {
    type Output;
    fn get(&self) -> Self::Output;
    fn on_change(&self, value: &Value);
}

pub struct DynamicConfRef<T> {
    pub key: String,
    pub value: RwLock<T>,
}

impl<T> DynamicConfRef<T>
where
    T: DeserializeOwned + Clone,
{
    pub fn new(key: &str, val: T) -> Self {
        Self {
            key: key.to_string(),
            value: RwLock::new(val),
        }
    }
}

impl<T> ConfRef<T> for DynamicConfRef<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Output = T;

    fn get(&self) -> T {
        let val = self.value.read();
        val.clone()
    }

    fn on_change(&self, val: &Value) {
        if let Ok(val) = serde_json::from_value::<T>(val.clone()) {
            let mut internal_val = self.value.write();
            *internal_val = val;
        }
    }
}

// =======================================================

pub struct StaticConfRef<T> {
    val: T,
}

impl<T> StaticConfRef<T> {
    pub fn new(val: T) -> Self {
        Self { val }
    }
}

impl<T> ConfRef<T> for StaticConfRef<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Output = T;

    fn get(&self) -> Self::Output {
        self.val.clone()
    }

    fn on_change(&self, value: &Value) {
        // nothing to do
    }
}

#[cfg(test)]
mod test {
    use crate::config_ref::{ConfRef, StaticConfRef};

    #[test]
    fn test_fixed_conf_ref() -> anyhow::Result<()> {
        let const_val = 42;
        let conf_ref = StaticConfRef::new(const_val);
        assert_eq!(conf_ref.get(), const_val);
        Ok(())
    }
}
