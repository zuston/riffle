use crate::app_manager::application_identifier::ApplicationId;
use std::fmt::{Display, Formatter};

#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
pub enum PurgeReason {
    SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(ApplicationId, i32),
    APP_LEVEL_EXPLICIT_UNREGISTER(ApplicationId),
    APP_LEVEL_HEARTBEAT_TIMEOUT(ApplicationId),
    SERVICE_FORCE_KILL(ApplicationId),
}

impl Display for PurgeReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(app_id, shuffle_id) => {
                write!(
                    f,
                    "SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(app_id={}, shuffle_id={})",
                    app_id, shuffle_id
                )
            }
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(app_id) => {
                write!(f, "APP_LEVEL_EXPLICIT_UNREGISTER(app_id={})", app_id)
            }
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(app_id) => {
                write!(f, "APP_LEVEL_HEARTBEAT_TIMEOUT(app_id={})", app_id)
            }
            PurgeReason::SERVICE_FORCE_KILL(app_id) => {
                write!(f, "SERVICE_FORCE_KILL(app_id={})", app_id)
            }
        }
    }
}

impl PurgeReason {
    pub fn extract(&self) -> (ApplicationId, Option<i32>) {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => (x.to_owned(), Some(*y)),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => (x.to_owned(), None),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => (x.to_owned(), None),
            PurgeReason::SERVICE_FORCE_KILL(x) => (x.to_owned(), None),
        }
    }

    pub fn extract_app_id(&self) -> ApplicationId {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => x.to_owned(),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => x.to_owned(),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => x.to_owned(),
            PurgeReason::SERVICE_FORCE_KILL(x) => x.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PurgeEvent {
    pub(crate) reason: PurgeReason,
}

#[cfg(test)]
mod tests {
    use crate::app_manager::application_identifier::ApplicationId;
    use crate::app_manager::purge_event::PurgeReason;

    #[test]
    fn test_display() {
        let reason =
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(ApplicationId::YARN(1, 1, 1), 1);
        assert_eq!(
            "SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(app_id=application_1_1_1, shuffle_id=1)",
            reason.to_string()
        );
    }
}
