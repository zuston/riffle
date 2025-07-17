use clap::builder::Str;
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// The configuration options related to riffle-servers on the Uniffle client side.
pub static SENDFILE_ENABLED_OPTION: Lazy<ClientConfigOption<bool>> = Lazy::new(|| {
    ClientConfigOption::key("spark.rss.client.urpcSendfileEnabled")
        .default_value(false)
        .with_description("This indicates whether the sendfile is enabled when urpc is activated")
});

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
    fn test_sendfile_enabled_option_default() {
        let conf = ClientRssConf {
            properties: HashMap::new(),
        };
        let result = conf.get(&SENDFILE_ENABLED_OPTION);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_sendfile_enabled_option_set_true() {
        let mut props = HashMap::new();
        props.insert(
            "spark.rss.client.urpcSendfileEnabled".to_string(),
            "true".to_string(),
        );
        let conf = ClientRssConf { properties: props };
        let result = conf.get(&SENDFILE_ENABLED_OPTION);
        assert_eq!(result, Some(true));
    }
}
