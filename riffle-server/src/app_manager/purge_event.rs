#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
pub enum PurgeReason {
    SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(String, i32),
    APP_LEVEL_EXPLICIT_UNREGISTER(String),
    APP_LEVEL_HEARTBEAT_TIMEOUT(String),
}

impl PurgeReason {
    pub fn extract(&self) -> (String, Option<i32>) {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => (x.to_owned(), Some(*y)),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => (x.to_owned(), None),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => (x.to_owned(), None),
        }
    }

    pub fn extract_app_id(&self) -> String {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => x.to_owned(),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => x.to_owned(),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => x.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PurgeEvent {
    pub(crate) reason: PurgeReason,
}
