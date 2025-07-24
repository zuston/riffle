use anyhow::{anyhow, Result};
use clap::builder::Str;
use log::warn;
use std::fmt::{Display, Formatter};

/// Optimized for faster hash-based lookups when using the app ID as a map key.
/// Also includes a fallback strategy to support other Spark application ID formats for compatibility.
const YARN_APP_PREFIX: &str = "application_";
const YARN_SPLIT_PREFIX: &str = "_";

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Hash)]
pub enum ApplicationId {
    YARN(u64, u32, u64),
    OTHERS(String),
}

impl Default for ApplicationId {
    fn default() -> Self {
        ApplicationId::YARN(1, 2, 2)
    }
}

impl ApplicationId {
    pub fn from(raw_app_id: &str) -> Self {
        Self::init(raw_app_id).unwrap()
    }

    pub fn mock() -> Self {
        ApplicationId::default()
    }

    // When app registering in the first time, the app id should be checked.
    pub fn init(raw_app_id: &str) -> Result<ApplicationId> {
        let id = match raw_app_id.strip_prefix(YARN_APP_PREFIX) {
            None => ApplicationId::OTHERS(raw_app_id.to_string()),
            Some(app_id) => {
                let parts: Vec<&str> = app_id.split(YARN_SPLIT_PREFIX).collect();
                if parts.len() != 3 {
                    warn!("Illegal app id: {} but fallback!", raw_app_id);
                    ApplicationId::OTHERS(raw_app_id.to_string())
                } else {
                    ApplicationId::YARN(
                        parts[0].parse().unwrap_or(0),
                        parts[1].parse().unwrap_or(0),
                        parts[2].parse().unwrap_or(0),
                    )
                }
            }
        };
        Ok(id)
    }
}

impl Display for ApplicationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            ApplicationId::YARN(p1, p2, p3) => {
                write!(
                    f,
                    "{YARN_APP_PREFIX}{}{YARN_SPLIT_PREFIX}{}{YARN_SPLIT_PREFIX}{}",
                    p1, p2, p3
                )
            }
            ApplicationId::OTHERS(raw) => {
                write!(f, "{raw}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_map_key() {
        let app_id = ApplicationId::mock();
        let mut hashmap: HashMap<ApplicationId, i32> = HashMap::new();
        hashmap.insert(app_id, 1);

        assert!(hashmap.contains_key(&ApplicationId::mock()));
    }

    #[test]
    fn test_application_id_parsing() {
        let app_id = ApplicationId::from("application_1747379850000_7237726_1749434628480");
        match app_id {
            ApplicationId::YARN(p1, p2, p3) => {
                assert_eq!(p1, 1747379850000);
                assert_eq!(p2, 7237726);
                assert_eq!(p3, 1749434628480);
            }
            ApplicationId::OTHERS(_) => {
                panic!()
            }
        }
    }

    #[test]
    fn test_application_id_display() {
        let app_id = ApplicationId::YARN(1747379850000, 7237726, 1749434628480);
        assert_eq!(
            app_id.to_string(),
            "application_1747379850000_7237726_1749434628480"
        );
    }

    #[test]
    fn test_local_app_id() {
        let raw = "local-456_23234";
        let id = ApplicationId::from(raw);
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn test_illegal_yarn_app_id() {
        let raw = "application_2323_2";
        let id = ApplicationId::from(raw);
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn test_unknown_app_id_should_pass() {
        let raw = "spark-my-app-0ffbc216fc3b4f4f8f0a437e8f93e087";
        let id = ApplicationId::from(raw);
        assert_eq!(id.to_string(), raw);
    }
}
