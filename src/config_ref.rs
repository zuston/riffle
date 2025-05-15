use crate::config_reconfigure::ReconfigurableConfManager;
use crate::util;
use bytesize::ByteSize;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type ConfigOption<T> = Arc<dyn ConfRef<T, Output = T>>;

/// The config_ref is to wrap the dynamic value retrieved by the specified key

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ByteString {
    pub parsed_val: u64,
}

unsafe impl Send for ByteString {}
unsafe impl Sync for ByteString {}

impl ByteString {
    pub fn new(raw_val: &str) -> Self {
        Self {
            parsed_val: util::parse_raw_to_bytesize(raw_val),
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.parsed_val
    }
}

impl Display for ByteString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.parsed_val)
    }
}

impl<'de> Deserialize<'de> for ByteString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let val = raw.parse::<ByteSize>().map_err(serde::de::Error::custom)?.0;
        Ok(ByteString { parsed_val: val })
    }
}

impl Into<u64> for ByteString {
    fn into(self) -> u64 {
        self.as_u64()
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
    fn with_callback(&self, callback: Box<dyn Fn(&T, &T) + Send + Sync>);
}

/// DynamicConfRef: A dynamic configuration reference that supports runtime updates.
pub struct DynamicConfRef<T> {
    pub key: String,
    pub value: RwLock<T>,
    pub callback: OnceCell<Box<dyn Fn(&T, &T) + Send + Sync>>,
}

impl<T> DynamicConfRef<T>
where
    T: DeserializeOwned + Clone,
{
    pub fn new(key: &str, val: T) -> Self {
        Self {
            key: key.to_string(),
            value: RwLock::new(val),
            callback: Default::default(),
        }
    }
}

impl<T> Into<ConfigOption<T>> for DynamicConfRef<T>
where
    T: Clone + Send + Sync + DeserializeOwned + 'static,
{
    fn into(self) -> ConfigOption<T> {
        Arc::new(self)
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
            let pre_val = &internal_val.deref().clone();
            *internal_val = val.clone();
            drop(internal_val); // Release the lock before invoking the callback

            if let Some(callback) = self.callback.get() {
                callback(pre_val, &val);
            }
        }
    }

    fn with_callback(&self, callback: Box<dyn Fn(&T, &T) + Send + Sync>) {
        self.callback.set(callback);
    }
}

/// StaticConfRef: A static configuration reference with a fixed value.
pub struct StaticConfRef<T> {
    val: T,
}

impl<T> StaticConfRef<T>
where
    T: Clone + Send + Sync + DeserializeOwned + 'static,
{
    pub fn new(val: T) -> Self {
        Self { val }
    }
}

impl<T> Into<ConfigOption<T>> for StaticConfRef<T>
where
    T: Clone + Send + Sync + DeserializeOwned + 'static,
{
    fn into(self) -> ConfigOption<T> {
        Arc::new(self)
    }
}

impl<T> ConfRef<T> for StaticConfRef<T>
where
    T: Clone + Send + Sync + DeserializeOwned + 'static,
{
    type Output = T;

    fn get(&self) -> Self::Output {
        self.val.clone()
    }

    fn on_change(&self, _value: &Value) {
        // No operation for StaticConfRef
    }

    fn with_callback(&self, _callback: Box<dyn Fn(&T, &T) + Send + Sync>) {
        // No operation for StaticConfRef
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_fixed_conf_ref() -> anyhow::Result<()> {
        let const_val = 42;
        let conf_ref = StaticConfRef::new(const_val);
        assert_eq!(conf_ref.get(), const_val);
        Ok(())
    }

    #[test]
    fn test_dynamic_conf_ref() -> anyhow::Result<()> {
        let mut conf_ref = DynamicConfRef::new("test_key", 42);
        assert_eq!(conf_ref.get(), 42);

        conf_ref.on_change(&serde_json::json!(100));
        assert_eq!(conf_ref.get(), 100);

        conf_ref.with_callback(Box::new(|old, new| {
            assert_eq!(*old, 100);
            assert_eq!(*new, 200);
        }));

        conf_ref.on_change(&serde_json::json!(200));

        Ok(())
    }
}
