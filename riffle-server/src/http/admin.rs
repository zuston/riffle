use crate::http::Handler;
use crate::server_state_manager::{ServerState, SERVER_STATE_MANAGER_REF};
use anyhow::Result;
use clap::builder::Str;
use poem::{handler, Request, RouteMethod};
use serde::Deserialize;
use strum_macros::{Display, EnumString};

#[derive(Default)]
pub struct AdminHandler;

impl Handler for AdminHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(admin_handler)
    }

    /// request with /admin?update_state=DECOMMISSION
    fn get_route_path(&self) -> String {
        "/admin".to_string()
    }
}

#[derive(Deserialize)]
struct OperationParam {
    update_state: Option<ServerState>,
    operation: Option<Operation>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Deserialize, EnumString, Display)]
enum Operation {
    KILL,
    FORCE_KILL,
}

#[handler]
fn admin_handler(req: &Request) -> poem::Result<String> {
    let params = req.params::<OperationParam>()?;
    let server_state_manager_ref = SERVER_STATE_MANAGER_REF.get();
    if server_state_manager_ref.is_none() {
        return Ok("Uninitialized server_state_manager. Ingore".to_string());
    }
    let server_state_manager_ref = server_state_manager_ref.unwrap();
    if let Some(operation) = params.operation {
        let force = match operation {
            Operation::KILL => false,
            Operation::FORCE_KILL => true,
        };
        server_state_manager_ref.shutdown(force);
    } else if let Some(state) = params.update_state {
        server_state_manager_ref.as_state(state);
    }

    Ok("Done".to_string())
}

#[cfg(test)]
mod tests {
    use crate::http::admin::AdminHandler;
    use crate::http::Handler;
    use poem::test::TestClient;
    use poem::Route;

    #[tokio::test]
    async fn test_router() {
        let handler = AdminHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli.get("/admin?update_state=HEALTHY").send().await;
        resp.assert_status_is_ok();
    }
}
