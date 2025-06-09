use clap::builder::Str;
use std::fmt::{Display, Formatter};

const APP_PREFIX: &str = "application_";
const SPLIT_PREFIX: &str = "_";

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Default, Hash)]
pub struct ApplicationId {
    part_1: u64,
    part_2: u32,
    part_3: u64,
}

impl ApplicationId {
    pub fn from(raw_app_id: &str) -> Self {
        let parts: Vec<&str> = raw_app_id
            .strip_prefix(APP_PREFIX)
            .unwrap_or(raw_app_id)
            .split(SPLIT_PREFIX)
            .collect();

        if parts.len() != 3 {
            panic!("Invalid application id format: {}", raw_app_id);
        }

        ApplicationId {
            part_1: parts[0].parse().unwrap_or(0),
            part_2: parts[1].parse().unwrap_or(0),
            part_3: parts[2].parse().unwrap_or(0),
        }
    }

    pub fn mock() -> Self {
        Self {
            part_1: 1,
            part_2: 2,
            part_3: 3,
        }
    }
}

impl Display for ApplicationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{APP_PREFIX}{}{SPLIT_PREFIX}{}{SPLIT_PREFIX}{}",
            self.part_1, self.part_2, self.part_3
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_id_parsing() {
        let app_id = ApplicationId::from("application_1747379850000_7237726_1749434628480");
        assert_eq!(app_id.part_1, 1747379850000);
        assert_eq!(app_id.part_2, 7237726);
        assert_eq!(app_id.part_3, 1749434628480);
    }

    #[test]
    fn test_application_id_display() {
        let app_id = ApplicationId {
            part_1: 1747379850000,
            part_2: 7237726,
            part_3: 1749434628480,
        };
        assert_eq!(
            app_id.to_string(),
            "application_1747379850000_7237726_1749434628480"
        );
    }

    #[test]
    #[should_panic(expected = "Invalid application id format")]
    fn test_invalid_format() {
        ApplicationId::from("invalid_format");
    }
}
