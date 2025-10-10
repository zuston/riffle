use crate::http::Handler;
use crate::server_state_manager::{ServerState, TransitionReason, SERVER_STATE_MANAGER_REF};
use anyhow::{anyhow, Result};
use clap::builder::Str;
use poem::http::StatusCode;
use poem::{handler, Request, RouteMethod};
use serde::Deserialize;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

#[derive(Default)]
pub struct AdminHandler;

impl Handler for AdminHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(admin_handler)
    }

    /// request with the following urls
    /// 1. /admin?kill
    /// 2. /admin?kill=force
    /// 3. /admin?get_state
    /// 4. /admin?update_state=DECOMMISSION
    fn get_route_path(&self) -> String {
        "/admin".to_string()
    }
}

#[handler]
async fn admin_handler(req: &Request) -> poem::Result<String> {
    let query = req.uri().query().unwrap_or("");
    let server_state_manager_ref = SERVER_STATE_MANAGER_REF.get();
    if server_state_manager_ref.is_none() {
        return Ok("Uninitialized server_state_manager. Ignore!".to_string());
    }
    let server_state_manager_ref = server_state_manager_ref.unwrap();

    let mut found_operation = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap();
        let value_opt = parts.next();

        match key {
            "kill" => {
                let force = value_opt.is_some() && value_opt.unwrap() == "force";
                server_state_manager_ref.shutdown(force).await?;
                found_operation = true;
            }
            "get_state" => {
                found_operation = true;
                return Ok(server_state_manager_ref.get_state().to_string());
            }
            "update_state" => {
                if let Some(raw_state) = value_opt {
                    if let Some(state) = ServerState::from_str(&raw_state.to_ascii_uppercase()).ok()
                    {
                        server_state_manager_ref.as_state(state, TransitionReason::ADMIN_HTTP_API);
                        found_operation = true;
                    } else {
                        return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                    }
                } else {
                    return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                }
            }
            _ => {
                return Ok("Invalid admin operation".to_string());
            }
        }
    }

    if !found_operation {
        return Ok("Invalid admin operation".to_string());
    }

    Ok("OK".to_string())
}

#[cfg(test)]
mod tests {
    use crate::app_manager::test::mock_config;
    use crate::app_manager::AppManager;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::http::admin::AdminHandler;
    use crate::http::Handler;
    use crate::runtime::manager::RuntimeManager;
    use crate::server_state_manager::{ServerStateManager, SERVER_STATE_MANAGER_REF};
    use crate::storage::StorageService;
    use poem::http::StatusCode;
    use poem::test::TestClient;
    use poem::Route;

    #[tokio::test]
    async fn test_router() {
        let config = mock_config();
        let runtime_manager: RuntimeManager = Default::default();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref = AppManager::get_ref(
            Default::default(),
            config.clone(),
            &storage,
            &reconf_manager,
        )
        .clone();
        let server_state_manager = ServerStateManager::new(&app_manager_ref, &config);
        let _ = SERVER_STATE_MANAGER_REF.set(server_state_manager.clone());

        let handler = AdminHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);

        // case1
        let resp = cli.get("/admin?get_state").send().await;
        resp.assert_status_is_ok();

        // case2
        let resp = cli.get("/admin?update_state=unhealthy").send().await;
        resp.assert_status_is_ok();

        // case3: illegal update_state request
        let resp = cli.get("/admin?update_state=unknown").send().await;
        resp.assert_status(StatusCode::BAD_REQUEST);
    }
}
